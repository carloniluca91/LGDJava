package it.carloni.luca.lgd.spark.step;

import it.carloni.luca.lgd.parameter.step.DataAUfficioValue;
import it.carloni.luca.lgd.schema.SofferenzePreviewSchema;
import it.carloni.luca.lgd.spark.common.AbstractStep;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import static it.carloni.luca.lgd.spark.utils.StepUtils.changeDateFormat;
import static it.carloni.luca.lgd.spark.utils.StepUtils.changeDateFormatUDF;

public class SofferenzePreview extends AbstractStep<DataAUfficioValue> {

    private final Logger logger = Logger.getLogger(SofferenzePreview.class);

    @Override
    public void run(DataAUfficioValue dataAUfficioValue) {

        String dataA = dataAUfficioValue.getDataA();
        String ufficio = dataAUfficioValue.getUfficio();

        logger.info(dataAUfficioValue);

        String soffOutDirCsv = getValue("sofferenze.preview.soff.outdir.csv");
        String soffGen2Path = getValue("sofferenze.preview.soff.gen2");
        String soffSintGen2Path = getValue("sofferenze.preview.soff.gen.sint2");

        logger.info("sofferenze.preview.soff.outdir.csv: " + soffOutDirCsv);
        logger.info("sofferenze.preview.soff.gen2: " + soffGen2Path);
        logger.info("sofferenze.preview.soff.gen.sint2: " + soffSintGen2Path);

        Dataset<Row> soffLoad = readCsvAtPathUsingSchema(soffOutDirCsv, SofferenzePreviewSchema.getSoffLoadPigSchema());

        // 37

        // '$ufficio'             as ufficio
        Column ufficioCol = functions.lit(ufficio).as("ufficio");

        // ToString(ToDate('$data_a','yyyyMMdd'),'yyyy-MM-dd') as datarif
        Column dataRifCol = functions.lit(changeDateFormat(dataA, "yyyyMMdd", "yyyy-MM-dd")).as("datarif");

        /*
        (double)REPLACE(saldoposizione,',','.')         as saldoposizione,
        (double)REPLACE(saldoposizionecontab,',','.')   as saldoposizionecontab
         */

        Column saldoPosizioneCol = replaceAndToDouble(soffLoad, "saldoposizione");
        Column saldoPosizioneContabCol = replaceAndToDouble(soffLoad, "saldoposizionecontab");

        Dataset<Row> soffBase = soffLoad.select(ufficioCol, dataRifCol, soffLoad.col("istituto"), soffLoad.col("ndg"),
                soffLoad.col("numerosofferenza"), soffLoad.col("datainizio"), soffLoad.col("datafine"),
                soffLoad.col("statopratica"), saldoPosizioneCol, saldoPosizioneContabCol);

        // 49

        // 51

        /*
        ToString(ToDate(datainizio,'yyyyMMdd'),'yyyy-MM-dd') as datainizio,
        ToString(ToDate(datafine,'yyyyMMdd'),'yyyy-MM-dd')   as datafine
         */

        String oldPattern = "yyyyMMdd";
        String newPattern = "yyyy-MM-dd";

        Column dataInizioCol = changeDateFormatUDF(soffBase.col("datainizio"), oldPattern, newPattern).alias("datainizio");
        Column dataFineCol = changeDateFormatUDF(soffBase.col("datafine"), oldPattern, newPattern).alias("datafine");

        // GROUP soff_base BY ( istituto, ndg, numerosofferenza );
        WindowSpec soffGen2WindowSpec = Window.partitionBy("istituto", "ndg", "numerosofferenza");

        /*
        SUM(soff_base.saldoposizione)        as saldoposizione,
        SUM(soff_base.saldoposizionecontab)  as saldoposizionecontab
         */

        Column saldoPosizioneSumCol = functions.sum(soffBase.col("saldoposizione")).over(soffGen2WindowSpec).as("saldoposizione");
        Column saldoPosizioneContabSumCol = functions.sum(soffBase.col("saldoposizionecontab")).over(soffGen2WindowSpec).as("saldoposizionecontab");

        Dataset<Row> soffGen2 = soffBase.select(soffBase.col("ufficio"), soffBase.col("datarif"),
                soffBase.col("istituto"), soffBase.col("ndg"), soffBase.col("numerosofferenza"),
                dataInizioCol, dataFineCol, soffBase.col("statopratica"),
                saldoPosizioneSumCol, saldoPosizioneContabSumCol);

        writeDatasetAsCsvAtPath(soffGen2, soffGen2Path);

        // 87

        // 89

        // GROUP soff_base BY ( ufficio, datarif, istituto, SUBSTRING(datainizio,0,6), SUBSTRING(datafine,0,6), statopratica );
        Column meseInizioCol = functions.substring(soffBase.col("datainizio"), 0, 6).as("mese_inizio");
        Column meseFineCol = functions.substring(soffBase.col("datafine"), 0, 6).as("mese_fine");

        Dataset<Row> soffSintGen2 = soffBase.groupBy(soffBase.col("ufficio"), soffBase.col("datarif"),
                soffBase.col("istituto"), meseInizioCol, meseFineCol, soffBase.col("statopratica"))
                .agg(functions.count(soffBase.col("*")).as("row_count"),
                        functions.sum(soffBase.col("saldoposizione")).as("saldoposizione"),
                        functions.sum(soffBase.col("saldoposizionecontab")).as("saldoposizionecontab"));

        writeDatasetAsCsvAtPath(soffSintGen2, soffSintGen2Path);
    }

    private Column replaceAndToDouble(Dataset<Row> df, String columnName){

        return functions.regexp_replace(df.col(columnName), ",", ".")
                .cast(DataTypes.DoubleType).as(columnName);
    }
}