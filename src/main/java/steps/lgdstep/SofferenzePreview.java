package steps.lgdstep;

import org.apache.commons.cli.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.StructType;
import steps.abstractstep.AbstractStep;

import java.nio.file.Paths;
import java.util.*;
import java.util.logging.Logger;

public class SofferenzePreview extends AbstractStep {

    private Logger logger = Logger.getLogger(this.getClass().getName());

    // required parameters
    private String ufficio;
    private String dataA;

    public SofferenzePreview(String[] args){

        // define options for $ufficio and  $data_a, then set them as required
        Option ufficioOption = new Option("u", "ufficio", true, "parametro $ufficio");
        Option dataAOption = new Option("dA", "dataA", true, "parametro $data_a");
        ufficioOption.setRequired(true);
        dataAOption.setRequired(true);

        // add the two options
        Options sofferenzePreviewOptions = new Options();
        sofferenzePreviewOptions.addOption(ufficioOption);
        sofferenzePreviewOptions.addOption(dataAOption);

        CommandLineParser parser = new BasicParser();

        // try to parse and retrieve command line arguments
        try{

            CommandLine cmdl = parser.parse(sofferenzePreviewOptions, args);
            ufficio = cmdl.getOptionValue("ufficio");
            dataA = cmdl.getOptionValue("dataA");
            logger.info("Arguments parsed correctly");
        }
        catch (ParseException e) {

            logger.info("ParseException: " + e.getMessage());
            ufficio = "ufficio_bpm";
            dataA = "20190101";

            logger.info("$ufficio: " + ufficio);
            logger.info("$data_a: " + dataA);
        }
    }

    @Override
    public void run() {

        String csvFormat = getProperty("csv_format");
        String sofferenzePreviewInputDir = getProperty("SOFFERENZE_PREVIEW_INPUT_DIR");
        String soffOutDirCsv = getProperty("SOFF_OUTDIR_CSV");

        logger.info("csvFormat: " + csvFormat);
        logger.info("sofferenzePreviewInputDir: " + sofferenzePreviewInputDir);
        logger.info("soffOutDirCsv: " + soffOutDirCsv);

        List<String> soffLoadColumnNames = Arrays.asList("istituto", "ndg", "numerosofferenza", "datainizio", "datafine",
                "statopratica", "saldoposizione", "saldoposizionecontab");
        StructType soffLoadSchema = getDfSchema(soffLoadColumnNames);
        Dataset<Row> soffLoad = sparkSession.read().format(csvFormat).option("delimiter", ",").schema(soffLoadSchema).csv(
                Paths.get(sofferenzePreviewInputDir, soffOutDirCsv).toString());

        // 37

        // '$ufficio'             as ufficio
        Column ufficioCol = functions.lit(ufficio).as("ufficio");

        // ToString(ToDate('$data_a','yyyyMMdd'),'yyyy-MM-dd') as datarif
        Column dataRifCol = castToDateCol(functions.lit(dataA), "yyyyMMdd", "yyyy-MM-dd").as("datarif");

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
        Column dataInizioCol = castToDateCol(soffBase.col("datainizio"), "yyyyMMdd", "yyyy-MM-dd").alias("datainizio");
        Column dataFineCol = castToDateCol(soffBase.col("datafine"), "yyyyMMdd", "yyyy-MM-dd").alias("datafine");

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

        String sofferenzePreviewOutputDir = getProperty("SOFFERENZE_PREVIEW_OUTPUT_DIR");
        String soffGen2Path = getProperty("SOFF_GEN_2");

        logger.info("sofferenzePreviewOutputDir: " + sofferenzePreviewOutputDir);
        logger.info("soffGen2Path: " + soffGen2Path);

        soffGen2.write().format(csvFormat).option("delimiter", ",").mode(SaveMode.Overwrite).csv(
                Paths.get(sofferenzePreviewOutputDir, soffGen2Path).toString());

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

        String soffSintGen2Path = getProperty("SOFF_GEN_SINT_2");
        logger.info("soffSintGen2Path: " + soffSintGen2Path);
        soffSintGen2.write().format(csvFormat).option("delimiter", ",").mode(SaveMode.Overwrite).csv(
                Paths.get(sofferenzePreviewOutputDir, soffSintGen2Path).toString());

        // 123
    }
}
