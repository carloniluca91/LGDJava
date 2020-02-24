package it.carloni.luca.lgd.steps;

import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import it.carloni.luca.lgd.common.AbstractStep;
import it.carloni.luca.lgd.schemas.SofferenzePreviewSchema;

import static it.carloni.luca.lgd.common.StepUtils.*;

public class SofferenzePreview extends AbstractStep {

    private final Logger logger = Logger.getLogger(SofferenzePreview.class);

    // required parameters
    private String ufficio;
    private String dataA;

    public SofferenzePreview(String ufficio, String dataA){

        this.ufficio = ufficio;
        this.dataA = dataA;
        stepInputDir = getValue("sofferenze.preview.input.dir");
        stepOutputDir = getValue("sofferenze.preview.output.dir");

        logger.debug("ufficio: " + this.ufficio);
        logger.debug("dataA: " + this.dataA);
        logger.debug("stepInputDir: " + stepInputDir);
        logger.debug("stepOutputDir: " + stepOutputDir);
    }

    @Override
    public void run() {

        String soffOutDirCsv = getValue("sofferenze.preview.soff.outdir.csv");
        String soffGen2Path = getValue("sofferenze.preview.soff.gen2");
        String soffSintGen2Path = getValue("sofferenze.preview.soff.gen.sint2");

        logger.debug("soffOutDirCsv: " + soffOutDirCsv);
        logger.debug("soffGen2Path: " + soffGen2Path);
        logger.debug("soffSintGen2Path: " + soffSintGen2Path);

        Dataset<Row> soffLoad = readCsvAtPathUsingSchema(soffOutDirCsv,
                fromPigSchemaToStructType(SofferenzePreviewSchema.getSoffLoadPigSchema()));

        // 37

        // '$ufficio'             as ufficio
        Column ufficioCol = functions.lit(ufficio).as("ufficio");

        // ToString(ToDate('$data_a','yyyyMMdd'),'yyyy-MM-dd') as datarif
        Column dataRifCol = functions.lit(changeDateFormat(dataA, "yyyyMMdd", "yyyy-MM-dd")).as("datarif");

        /*
        (double)REPLACE(saldoposizione,',','.')         as saldoposizione,
        (double)REPLACE(saldoposizionecontab,',','.')   as saldoposizionecontab
         */

        Column saldoPosizioneCol = replaceAndConvertToDouble(soffLoad, "saldoposizione", ",", ".").as("saldoposizione");
        Column saldoPosizioneContabCol = replaceAndConvertToDouble(soffLoad, "saldoposizionecontab", ",", ".").as("saldoposizionecontab");

        Dataset<Row> soffBase = soffLoad.select(ufficioCol, dataRifCol, soffLoad.col("istituto"), soffLoad.col("ndg"),
                soffLoad.col("numerosofferenza"), soffLoad.col("datainizio"), soffLoad.col("datafine"),
                soffLoad.col("statopratica"), saldoPosizioneCol, saldoPosizioneContabCol);

        // 49

        // 51

        /*
        ToString(ToDate(datainizio,'yyyyMMdd'),'yyyy-MM-dd') as datainizio,
        ToString(ToDate(datafine,'yyyyMMdd'),'yyyy-MM-dd')   as datafine
         */
        Column dataInizioCol = changeDateFormat(soffBase.col("datainizio"), "yyyyMMdd", "yyyy-MM-dd").alias("datainizio");
        Column dataFineCol = changeDateFormat(soffBase.col("datafine"), "yyyyMMdd", "yyyy-MM-dd").alias("datafine");

        // GROUP soff_base BY ( istituto, ndg, numerosofferenza );
        WindowSpec soffGen2WindowSpec = Window.partitionBy(
                soffBase.col("istituto"),
                soffBase.col("ndg"),
                soffBase.col("numerosofferenza"));

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
                .agg(functions.count(soffBase.col("saldoposizione")).as("row_count"),
                        functions.sum(soffBase.col("saldoposizione")).as("saldoposizione"),
                        functions.sum(soffBase.col("saldoposizionecontab")).as("saldoposizionecontab"));

        writeDatasetAsCsvAtPath(soffSintGen2, soffSintGen2Path);
    }
}
