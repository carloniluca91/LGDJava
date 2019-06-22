package steps;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.*;

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

        // 22
        String tlbcdefPath = getProperty("TLBCDEF_PATH");
        logger.info("tlbcdefPath: " + tlbcdefPath);
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
        String csvFormat = getProperty("CSV_FORMAT");  // retrieve csv_format from config.properties
        logger.info("csv format: " + csvFormat);
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
    }
}
