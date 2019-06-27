package steps;

import org.apache.commons.cli.*;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.*;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;


class CicliLavStep1 extends AbstractStep{

    private String dataDa;
    private String dataA;

    CicliLavStep1(String[] args) {

        // define options dataDa and dataA and set them as required
        Option dataDaOption = new Option("dd", "dataDa", true, "parametro dataDa");
        Option dataAOption = new Option("da", "dataA", true, "parametro dataA");
        dataDaOption.setRequired(true);
        dataAOption.setRequired(true);

        // add the two previously defined options
        Options options = new Options();
        options.addOption(dataDaOption);
        options.addOption(dataAOption);

        CommandLineParser parser = new BasicParser();

        // try to parse and retrieve command line arguments
        try {
            CommandLine cmd = parser.parse(options, args);
            dataDa = cmd.getOptionValue("dataDa");
            dataA = cmd.getOptionValue("dataA");

        } catch (ParseException e) {

            logger.info("ParseException: " + e.getMessage());
            dataDa = "2015-01-01";
            dataA = "2019-01-01";
            logger.info("Setting dataA to :" + dataDa);
            logger.info("Setting dataA to :" + dataA);
        }
    }

    void run(){

        logger.info("dataDa: " + dataDa + ", dataA: " + dataA);

        // retrieve csv_format, input data directory and file name from configuration.properties file
        String csvFormat = getProperty("csv_format");
        String ciclilavStep1InputDir = getProperty("CICLILAV_STEP1_INPUT_DIR");
        String tlbcidef_name = getProperty("TLBCIDEF_CSV");

        logger.info("csv format: " + csvFormat);
        logger.info("tlbcdefPath: " + ciclilavStep1InputDir);
        logger.info("tlbcidef_name: " +  tlbcidef_name);

        String tlbcdefPath = Paths.get(ciclilavStep1InputDir, tlbcidef_name).toString();
        logger.info("tlbcdefPath: " + tlbcdefPath);

        // 22
        StructField[] tlbcidefColumns = new StructField[]{

                new StructField("cd_isti", DataTypes.StringType, true, Metadata.empty()),
                new StructField("ndg_principale", DataTypes.StringType, true, Metadata.empty()),
                new StructField("cod_cr", DataTypes.StringType, true, Metadata.empty()),
                new StructField("dt_inizio_ciclo", DataTypes.StringType, true, Metadata.empty()),
                new StructField("dt_ingresso_status", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("status_ingresso", DataTypes.StringType, true, Metadata.empty()),
                new StructField("dt_uscita_status", DataTypes.StringType, true, Metadata.empty()),
                new StructField("status_uscita", DataTypes.StringType, true, Metadata.empty()),
                new StructField("dt_fine_ciclo", DataTypes.StringType, true, Metadata.empty()),
                new StructField("indi_pastdue", DataTypes.StringType, true, Metadata.empty()),
                new StructField("indi_impr_priv", DataTypes.StringType, true, Metadata.empty())};

        StructType tlbcidefSchema = new StructType(tlbcidefColumns);
        Dataset<Row> tlbcidef = sparkSession.read().format(csvFormat).option("delimiter", ",")
                .schema(tlbcidefSchema).csv(tlbcdefPath);

        // 37

        // 40
        // column condition that selects records with dataDa<= dt_inizio ciclo <= dataA
        Column dtInizioCicloFilterCol = functions.unix_timestamp(functions.col("dt_inizio_ciclo"), "yyyyMMdd")
                .between(functions.unix_timestamp(functions.lit(dataDa), "yyyy-MM-dd"),
                        functions.unix_timestamp(functions.lit(dataA), "yyyy-MM-dd"));
        logger.info("Filter condition for dt_inizio_ciclo: " + dtInizioCicloFilterCol.toString());

        Column statusIngressoTrimCol = functions.trim(functions.col("status_ingresso"));

        // definition of column datainiziopd
        Column dataInizioPdCol = functions.when(statusIngressoTrimCol.equalTo(functions.lit("PASTDUE")),
                functions.col("dt_ingresso_status")).otherwise(null);
        logger.info("dataInizioPdCol: " + dataInizioPdCol);

        // definition of column datainizioinc
        Column dataInizioIncCol = functions.when(statusIngressoTrimCol.equalTo(functions.lit("INCA")).or(
                statusIngressoTrimCol.equalTo(functions.lit("INADPRO"))), functions.col("dt_ingresso_status"))
                .otherwise(null);
        logger.info("dataInizioIncCol: " + dataInizioIncCol.toString());

        // definition of column datainizioristrutt
        Column dataInizioRistruttCol = functions.when(statusIngressoTrimCol.equalTo(functions.lit("RISTR")),
                functions.col("dt_ingresso_status")).otherwise(null);
        logger.info("dataInizioRistruttCol: " + dataInizioRistruttCol.toString());

        // definition of column datainiziosoff
        Column dataInizioSoffCol = functions.when(statusIngressoTrimCol.equalTo(functions.lit("SOFF")),
                functions.col("dt_ingresso_status")).otherwise(null);
        logger.info("dataInizioSoffCol: " + dataInizioSoffCol.toString());

        Dataset<Row> tlbcidefUnpivot = tlbcidef.filter(dtInizioCicloFilterCol).select(functions.col("cd_isti"),
                functions.col("ndg_principale"), functions.col("dt_inizio_ciclo"), functions.col("dt_fine_ciclo"),
                dataInizioPdCol.as("datainiziopd"), dataInizioIncCol.as("datainizioinc"),
                dataInizioRistruttCol.as("datainizioristrutt"), dataInizioSoffCol.as("datainiziosoff"));

        // 55
        Dataset<Row> tlbcidefMax = tlbcidefUnpivot.groupBy(functions.col("cd_isti"),
                functions.col("ndg_principale"), functions.col("dt_inizio_ciclo")).agg(
                        functions.max("dt_fine_ciclo").as("dt_fine_ciclo"),
                        functions.min("datainiziopd").as("datainiziopd"),
                        functions.min("datainizioristrutt").as("datainizioristrutt"),
                        functions.min("datainizioinc").as("datainizioinc"),
                        functions.min("datainiziosoff").as("datainiziosoff"));
        // 71

        // 78
        String tlbcraccPath = Paths.get(ciclilavStep1InputDir, getProperty("TLBCRACC_CSV")).toString();
        logger.info("tlbcraccPath: " + tlbcdefPath);
        StructField[] tlbcraccColumns = new StructField[]{

                new StructField("data_rif", DataTypes.StringType, true, Metadata.empty()),
                new StructField("cd_isti", DataTypes.StringType, true, Metadata.empty()),
                new StructField("ndg", DataTypes.StringType, true, Metadata.empty()),
                new StructField("cod_raccordo", DataTypes.StringType, true, Metadata.empty()),
                new StructField("data_val", DataTypes.StringType, true, Metadata.empty())};

        StructType tlbcraccSchema = new StructType(tlbcraccColumns);
        Dataset<Row> tlbcraccLoad = sparkSession.read().format(csvFormat).option("delimiter", ",")
                .schema(tlbcraccSchema).csv(tlbcraccPath);

        Column tlbraccFilterCol = functions.unix_timestamp(functions.col("data_rif"), "yyyy-MM-dd").lt(
                functions.when(functions.unix_timestamp(functions.lit(dataA), "yyyy-MM-dd").lt(
                        functions.unix_timestamp(functions.lit("2015-07-31"), "yyyy-MM-dd")),
                        functions.unix_timestamp(functions.lit("2015-07-31"), "yyyy-MM-dd")).otherwise(
                                functions.unix_timestamp(functions.lit(dataA), "yyyy-MM-dd")));
        logger.info("tlbraccFilterCol: " + tlbraccFilterCol.toString());

        Dataset<Row> tlbcracc = tlbcraccLoad.filter(tlbraccFilterCol);
        Dataset<Row> tlbcraccClone1 = tlbcracc.toDF().withColumnRenamed("cd_isti", "cd_isti_clone");
        Dataset<Row> tlbcraccClone2 = tlbcraccClone1.withColumnRenamed("ndg", "ndg_clone");
        // 90

        // 97
        Dataset<Row> cicliRacc1 = tlbcidefMax.join(tlbcracc, tlbcidefMax.col("cd_isti").equalTo(tlbcracc.col("cd_isti"))
                .and(tlbcidefMax.col("ndg_principale").equalTo(tlbcracc.col("ndg"))), "left").select(
                        tlbcidefMax.col("cd_isti"), tlbcidefMax.col("ndg_principale"), tlbcidefMax.col("cd_isti"),
                        tlbcidefMax.col("dt_inizio_ciclo"), tlbcidefMax.col("dt_fine_ciclo"), tlbcidefMax.col("datainiziopd"),
                        tlbcidefMax.col("datainizioristrutt"), tlbcidefMax.col("datainizioinc"), tlbcidefMax.col("datainiziosoff"),
                        tlbcracc.col("cod_raccordo"), tlbcracc.col("data_rif"));
        // 110

        // 119
        List<String> joinCols = Arrays.asList("cod_raccordo", "data_rif");
        logger.info("join columns: " + joinCols.toString());
        logger.info("convertion to scala Seq");
        Seq<String> joinColsSeq = JavaConverters.asScalaIteratorConverter(joinCols.iterator()).asScala().toSeq();  // conversion to scala Seq

        Column cdIstiCedCol = functions.when(tlbcraccClone2.col("cd_isti_clone").isNotNull(), tlbcraccClone2.col("cd_isti_clone"))
                .otherwise(cicliRacc1.col("cd_isti")).as("cd_isti_ced");
        logger.info("cdIstiCedCol: " + cdIstiCedCol.toString());

        Column ndgCedCol = functions.when(tlbcraccClone2.col("ndg_clone").isNotNull(), tlbcraccClone2.col("ndg_clone"))
                .otherwise(cicliRacc1.col("ndg_principale")).as("ndg_ced");
        logger.info("ndgCedCol: " + ndgCedCol.toString());

        logger.info("cicliRacc1 columns: " + Arrays.toString(cicliRacc1.columns()));
        logger.info("tlbcraccClone columns: " + Arrays.toString(tlbcracc.columns()));

        Dataset<Row> ciclilavStep1 = cicliRacc1.join(tlbcraccClone2, joinColsSeq, "left").select(
                cicliRacc1.col("cd_isti"), cicliRacc1.col("ndg_principale"), cicliRacc1.col("dt_inizio_ciclo"),
                cicliRacc1.col("dt_fine_ciclo"), cicliRacc1.col("datainiziopd"), cicliRacc1.col("datainizioristrutt"),
                cicliRacc1.col("datainizioinc"), cicliRacc1.col("datainiziosoff"), functions.lit(0).as("progr"),
                cdIstiCedCol, ndgCedCol).distinct();

        // 149

        // 155
        Column dtRifCraccCol = functions.when(tlbcracc.col("data_rif").isNotNull(), tlbcracc.col("data_rif"))
                .otherwise(cicliRacc1.col("dt_inizio_ciclo")).as("dt_rif_cracc");
        logger.info("dtRifCraccCol: " + dtRifCraccCol.toString());

        Dataset<Row> ciclilavStep1Filecracc = cicliRacc1.join(tlbcracc, joinColsSeq, "left").select(
                cicliRacc1.col("cd_isti"), cicliRacc1.col("ndg_principale"), cicliRacc1.col("dt_inizio_ciclo"),
                cicliRacc1.col("dt_fine_ciclo")//,cdIstiCedCol, ndgCedCol, dtRifCraccCol);
        );
        // 176

        String ciclilavStep1OutputDir = getProperty("CICLILAV_STEP1_OUTPUT_DIR");
        String ciclilavStep1OutCsv = getProperty("CICLILAV_STEP1_OUT_CSV");
        logger.info("ciclilavStep1OutputDir: " + ciclilavStep1OutputDir);
        logger.info("ciclilavStep1OutCsv: " + ciclilavStep1OutCsv);

        String ciclilavStep1FilecraccCsv = getProperty("CICLILAV_STEP1_FILECRACC_CSV");
        logger.info("ciclilavStep1FilecraccCsv: " + ciclilavStep1FilecraccCsv);

        ciclilavStep1.write().format(csvFormat).option("delimiter", ",").csv(
                Paths.get(ciclilavStep1OutputDir, ciclilavStep1OutCsv).toString());
        ciclilavStep1Filecracc.write().format(csvFormat).option("delimiter", ",").csv(
                Paths.get(ciclilavStep1OutputDir, ciclilavStep1FilecraccCsv).toString());
    }
}
