package steps.lgdstep;

import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.StructType;
import steps.abstractstep.AbstractStep;

import java.nio.file.Paths;
import java.util.*;

public class SofferenzePreview extends AbstractStep {

    // required parameters
    private String ufficio;
    private String dataA;

    public SofferenzePreview(String loggerName, String ufficio, String dataA){

        super(loggerName);
        logger = Logger.getLogger(loggerName);

        this.ufficio = ufficio;
        this.dataA = dataA;

        stepInputDir = getPropertyValue("sofferenze.preview.input.dir");
        stepOutputDir = getPropertyValue("sofferenze.preview.output.dir");

        logger.info("stepInputDir: " + stepInputDir);
        logger.info("stepOutputDir: " + stepOutputDir);
        logger.info("ufficio: " + this.ufficio);
        logger.info("dataA: " + this.dataA);
    }

    @Override
    public void run() {

        String csvFormat = getPropertyValue("csv.format");
        String soffOutDirCsv = getPropertyValue("soff.outdir.csv");

        logger.info("csvFormat: " + csvFormat);
        logger.info("soffOutDirCsv: " + soffOutDirCsv);

        List<String> soffLoadColumnNames = Arrays.asList("istituto", "ndg", "numerosofferenza", "datainizio", "datafine",
                "statopratica", "saldoposizione", "saldoposizionecontab");
        StructType soffLoadSchema = getStringTypeSchema(soffLoadColumnNames);
        Dataset<Row> soffLoad = sparkSession.read().format(csvFormat).option("delimiter", ",").schema(soffLoadSchema).csv(
                Paths.get(stepInputDir, soffOutDirCsv).toString());

        // 37

        // '$ufficio'             as ufficio
        Column ufficioCol = functions.lit(ufficio).as("ufficio");

        // ToString(ToDate('$data_a','yyyyMMdd'),'yyyy-MM-dd') as datarif
        Column dataRifCol = dateFormat(functions.lit(dataA), "yyyyMMdd", "yyyy-MM-dd").as("datarif");

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
        Column dataInizioCol = dateFormat(soffBase.col("datainizio"), "yyyyMMdd", "yyyy-MM-dd").alias("datainizio");
        Column dataFineCol = dateFormat(soffBase.col("datafine"), "yyyyMMdd", "yyyy-MM-dd").alias("datafine");

        // GROUP soff_base BY ( istituto, ndg, numerosofferenza );
        WindowSpec soffGen2Window = Window.partitionBy(
                soffBase.col("istituto"), soffBase.col("ndg"), soffBase.col("numerosofferenza"));

        /*
        SUM(soff_base.saldoposizione)        as saldoposizione,
        SUM(soff_base.saldoposizionecontab)  as saldoposizionecontab
         */

        Column saldoPosizioneSumCol = functions.sum(soffBase.col("saldoposizione")).over(soffGen2Window).as("saldoposizione");
        Column saldoPosizioneContabSumCol = functions.sum(soffBase.col("saldoposizionecontab")).over(soffGen2Window).as("saldoposizionecontab");

        Dataset<Row> soffGen2 = soffBase.select(soffBase.col("ufficio"), soffBase.col("datarif"),
                soffBase.col("istituto"), soffBase.col("ndg"), soffBase.col("numerosofferenza"),
                dataInizioCol, dataFineCol, soffBase.col("statopratica"),
                saldoPosizioneSumCol, saldoPosizioneContabSumCol);

        String soffGen2Path = getPropertyValue("soff.gen2");
        logger.info("soffGen2Path: " + soffGen2Path);

        soffGen2.write().format(csvFormat).option("delimiter", ",").mode(SaveMode.Overwrite).csv(
                Paths.get(stepOutputDir, soffGen2Path).toString());

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

        String soffSintGen2Path = getPropertyValue("soff.gen.sint2");
        logger.info("soffSintGen2Path: " + soffSintGen2Path);
        soffSintGen2.write().format(csvFormat).option("delimiter", ",").mode(SaveMode.Overwrite).csv(
                Paths.get(stepOutputDir, soffSintGen2Path).toString());

        // 123
    }
}
