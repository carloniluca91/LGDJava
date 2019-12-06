package steps.lgdstep;

import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import scala.collection.Seq;
import steps.abstractstep.AbstractStep;

import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

public class CicliLavStep1 extends AbstractStep {

    // required parameters
    private String dataDa;
    private String dataA;

    public CicliLavStep1(String loggerName, String dataDa, String dataA) {

        super(loggerName);

        logger = Logger.getLogger(loggerName);

        this.dataDa = dataDa;
        this.dataA = dataA;

        stepInputDir = getLGDPropertyValue("ciclilav.step1.input.dir");
        stepOutputDir = getLGDPropertyValue("ciclilav.step1.output.dir");

        logger.debug("stepInputDir: " + stepInputDir);
        logger.debug("stepOutputDir: " + stepOutputDir);
        logger.debug("dataDa: " + this.dataDa);
        logger.debug("dataA: " + this.dataA);
    }

    public void run(){

        // retrieve csv_format, input data directory and file name from configuration.properties file
        String csvFormat = getLGDPropertyValue("csv.format");
        String tlbcidefCsv = getLGDPropertyValue("tlbcidef.csv");
        String tlbcraccCsv = getLGDPropertyValue("tlbcracc.csv");
        String tlbcdefPath = Paths.get(stepInputDir, tlbcidefCsv).toString();

        logger.debug("csv format: " + csvFormat);
        logger.debug("tlbcidefCsv: " +  tlbcidefCsv);
        logger.debug("tlbcraccCsv: " + tlbcraccCsv);
        logger.debug("tlbcdefPath: " + tlbcdefPath);

        // 22
        List<String> tlbcidefColumns = Arrays.asList("cd_isti", "ndg_principale", "cod_cr", "dt_inizio_ciclo", "dt_ingresso_status",
                "status_ingresso", "dt_uscita_status", "status_uscita", "dt_fine_ciclo", "indi_pastdue", "indi_impr_priv");
        StructType tlbcidefSchema = getStringTypeSchema(tlbcidefColumns);
        Dataset<Row> tlbcidef = sparkSession.read().format(csvFormat).option("delimiter", ",")
                .schema(tlbcidefSchema).csv(tlbcdefPath);

        // 37

        // 40
        // FILTER tlbcidef BY dt_inizio_ciclo >= $data_da AND dt_inizio_ciclo <= $data_a;
        Column dtInizioCicloFilterCol = dateBetween(tlbcidef.col("dt_inizio_ciclo"), "yyyyMMdd",
                dataDa, dataDaPattern, dataA, dataAPattern);

        Column statusIngressoTrimCol = functions.trim(functions.col("status_ingresso"));

        // (TRIM(status_ingresso)=='PASTDUE'?dt_ingresso_status:null) as datainiziopd,
        Column dataInizioPdCol = functions.when(statusIngressoTrimCol.equalTo(functions.lit("PASTDUE")),
                functions.col("dt_ingresso_status")).otherwise(null).as("datainiziopd");

        // (TRIM(status_ingresso)=='INCA' or TRIM(status_ingresso)=='INADPRO'?dt_ingresso_status:null) as datainizioinc,
        Column dataInizioIncCol = functions.when(statusIngressoTrimCol.equalTo(functions.lit("INCA")).or(
                statusIngressoTrimCol.equalTo(functions.lit("INADPRO"))), functions.col("dt_ingresso_status"))
                .otherwise(null).as("datainizioinc");

        // (TRIM(status_ingresso)=='RISTR'?dt_ingresso_status:null) as datainizioristrutt,
        Column dataInizioRistruttCol = functions.when(statusIngressoTrimCol.equalTo(functions.lit("RISTR")),
                functions.col("dt_ingresso_status")).otherwise(null).as("datainizioristrutt");

        // (TRIM(status_ingresso)=='SOFF'?dt_ingresso_status:null) as datainiziosoff
        Column dataInizioSoffCol = functions.when(statusIngressoTrimCol.equalTo(functions.lit("SOFF")),
                functions.col("dt_ingresso_status")).otherwise(null).as("datainiziosoff");

        Dataset<Row> tlbcidefUnpivot = tlbcidef.filter(dtInizioCicloFilterCol).select(functions.col("cd_isti"),
                functions.col("ndg_principale"), functions.col("dt_inizio_ciclo"), functions.col("dt_fine_ciclo"),
                dataInizioPdCol, dataInizioIncCol, dataInizioRistruttCol, dataInizioSoffCol);

        // 55
        Dataset<Row> tlbcidefMax = tlbcidefUnpivot.groupBy(
                functions.col("cd_isti"), functions.col("ndg_principale"), functions.col("dt_inizio_ciclo"))
                .agg(functions.max("dt_fine_ciclo").as("dt_fine_ciclo"),
                        functions.min("datainiziopd").as("datainiziopd"),
                        functions.min("datainizioristrutt").as("datainizioristrutt"),
                        functions.min("datainizioinc").as("datainizioinc"),
                        functions.min("datainiziosoff").as("datainiziosoff"));
        // 71

        // 78
        String tlbcraccPath = Paths.get(stepInputDir, tlbcraccCsv).toString();
        List<String> tlbcraccColumns = Arrays.asList("data_rif", "cd_isti", "ndg", "cod_raccordo", "data_val");
        StructType tlbcraccSchema = getStringTypeSchema(tlbcraccColumns);
        Dataset<Row> tlbcraccLoad = sparkSession.read().format(csvFormat).option("delimiter", ",")
                .schema(tlbcraccSchema).csv(tlbcraccPath);

        // FILTER tlbcracc_load BY data_rif <= ( (int)$data_a <= 20150731 ? 20150731 : (int)$data_a );
        LocalDate defaultDataA = parseStringToLocalDate("20150731", "yyyyMMdd");
        LocalDate dataADate = parseStringToLocalDate(dataA, dataAPattern);
        String greatestDateString = dataADate.compareTo(defaultDataA) <= 0 ?
                defaultDataA.format(DateTimeFormatter.ofPattern(dataAPattern)) : dataADate.format(DateTimeFormatter.ofPattern(dataDaPattern));

        // FILTER tlbcracc_load BY data_rif <= ( (int)$data_a <= 20150731 ? 20150731 : (int)$data_a );
        Column tlbraccFilterCol = dateLeqOtherDate(tlbcraccLoad.col("data_rif"),"yyyy-MM-dd",
                greatestDateString, "yyyy-MM-dd");

        Dataset<Row> tlbcracc = tlbcraccLoad.filter(tlbraccFilterCol);

        // clone tlbcracc to avoid Analysis exception
        Dataset<Row> tlbcraccClone = tlbcracc.toDF().withColumnRenamed("cd_isti", "cd_isti_clone")
                .withColumnRenamed("ndg", "ndg_clone");
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
        Seq<String> joinColsSeq = toScalaStringSeq(Arrays.asList("cod_raccordo", "data_rif"));  // conversion to scala Seq

        // (tlbcracc::cd_isti is not null ? tlbcracc::cd_isti : cicli_racc_1::cd_isti) as cd_isti_ced
        Column cdIstiCedCol = functions.when(tlbcraccClone.col("cd_isti_clone").isNotNull(), tlbcraccClone.col("cd_isti_clone"))
                .otherwise(cicliRacc1.col("cd_isti")).as("cd_isti_ced");

        // (tlbcracc::ndg     is not null ? tlbcracc::ndg     : cicli_racc_1::ndg_principale) as ndg_ced
        Column ndgCedCol = functions.when(tlbcraccClone.col("ndg_clone").isNotNull(), tlbcraccClone.col("ndg_clone"))
                .otherwise(cicliRacc1.col("ndg_principale")).as("ndg_ced");

        Dataset<Row> ciclilavStep1 = cicliRacc1.join(tlbcraccClone, joinColsSeq, "left").select(
                cicliRacc1.col("cd_isti"), cicliRacc1.col("ndg_principale"), cicliRacc1.col("dt_inizio_ciclo"),
                cicliRacc1.col("dt_fine_ciclo"), cicliRacc1.col("datainiziopd"), cicliRacc1.col("datainizioristrutt"),
                cicliRacc1.col("datainizioinc"), cicliRacc1.col("datainiziosoff"),
                functions.lit(0).as("progr"), cdIstiCedCol, ndgCedCol).distinct();

        // 149

        // 155
        // (tlbcracc::data_rif is not null ? tlbcracc::data_rif : cicli_racc_1::dt_inizio_ciclo) as dt_rif_cracc
        Column dtRifCraccCol = functions.when(tlbcraccClone.col("data_rif").isNotNull(), tlbcraccClone.col("data_rif"))
                .otherwise(cicliRacc1.col("dt_inizio_ciclo")).as("dt_rif_cracc");

        Dataset<Row> ciclilavStep1Filecracc = cicliRacc1.join(tlbcraccClone, joinColsSeq, "left").select(
                cicliRacc1.col("cd_isti"), cicliRacc1.col("ndg_principale"), cicliRacc1.col("dt_inizio_ciclo"),
                cicliRacc1.col("dt_fine_ciclo"), cdIstiCedCol, ndgCedCol, dtRifCraccCol);
        // 176

        String ciclilavStep1OutCsv = getLGDPropertyValue("ciclilav.step1.out.csv");
        String ciclilavStep1FilecraccCsv = getLGDPropertyValue("ciclilav.step1.filecracc.csv");

        logger.debug("ciclilavStep1OutCsv: " + ciclilavStep1OutCsv);
        logger.debug("ciclilavStep1FilecraccCsv: " + ciclilavStep1FilecraccCsv);

        ciclilavStep1.write().format(csvFormat).option("delimiter", ",").mode(SaveMode.Overwrite).csv(
                Paths.get(stepOutputDir, ciclilavStep1OutCsv).toString());

        ciclilavStep1Filecracc.write().format(csvFormat).option("delimiter", ",").mode(SaveMode.Overwrite).csv(
                Paths.get(stepOutputDir, ciclilavStep1FilecraccCsv).toString());
    }
}
