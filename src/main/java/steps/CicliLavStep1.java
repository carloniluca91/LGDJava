package steps;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.*;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

public class CicliLavStep1 extends AbstractStep{

    private final Options options = new Options();
    private final Logger logger = Logger.getLogger(CicliLavStep1.class.getName());

    public CicliLavStep1(String[] parameters) {

        super(parameters);
        Option dataDa = new Option("dd", "dataDa", true, "parametro dataDa");
        Option dataA = new Option("da", "dataA", true, "parametro dataA");
        dataDa.setRequired(true);
        dataA.setRequired(true);
        options.addOption(dataDa);
        options.addOption(dataA);
    }

    public void run(){

        String dataDa = options.getOption("dd").getValue();
        String dataA = options.getOption("da").getValue();
        logger.info("dataDa: " + dataDa + ", dataA: " + dataA);

        String csvFormat = getProperty("csv_format");  // retrieve csv_format from config.properties
        logger.info("csv format: " + csvFormat);

        String tlbcdefPath = getProperty("TLBCDEF_PATH");
        logger.info("tlbcdefPath: " + tlbcdefPath);

        // 22
        StructField[] tlbcidefColumns = new StructField[]{

                new StructField("cd_isti", DataTypes.StringType, true, Metadata.empty()),
                new StructField("ndg_principale", DataTypes.StringType, true, Metadata.empty()),
                new StructField("cod_cr", DataTypes.StringType, true, Metadata.empty()),
                new StructField("dt_inizio_ciclo", DataTypes.IntegerType, true, Metadata.empty()),
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
        Column dtInizioCicloFilterCol = functions.unix_timestamp(functions.col("dt_inizio_ciclo"), "yyyy-MM-dd")
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
                functions.col("ndg_principale"), functions.col("dt_fine_ciclo"),
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
        String tlbcraccPath = getProperty("TLBCRACC_PATH");
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

        Column cdIstiCedCol = functions.when(tlbcracc.col("cd_isti").isNotNull(), tlbcracc.col("cd_isti"))
                .otherwise(cicliRacc1.col("cd_isti")).as("cd_isti_ced");
        logger.info("cdIstiCedCol: " + cdIstiCedCol.toString());

        Column ndgCedCol = functions.when(tlbcracc.col("ndg").isNotNull(), tlbcracc.col("ndg"))
                .otherwise(cicliRacc1.col("ndg_principale")).as("ndg_ced");
        logger.info("ndgCedCol: " + ndgCedCol.toString());

        Dataset<Row> ciclilavStep1 = cicliRacc1.join(tlbcracc, joinColsSeq, "left").select(
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
                cicliRacc1.col("dt_fine_ciclo"), cdIstiCedCol, ndgCedCol, dtRifCraccCol);
        // 176

        String ciclilavStep1Outdir = getProperty("CICLILAV_STEP1_OUTDIR");
        logger.info("ciclilavStep1Outdir: " + ciclilavStep1Outdir);

        String ciclilavStep1Outcracc = getProperty("CICLILAV_STEP1_OUTCRACC");
        logger.info("ciclilavStep1Outcracc: " + ciclilavStep1Outcracc);

        ciclilavStep1.write().format(csvFormat).option("delimiter", ",").csv(ciclilavStep1Outdir);
        ciclilavStep1Filecracc.write().format(csvFormat).option("delimiter", ",").csv(ciclilavStep1Outcracc);
    }
}
