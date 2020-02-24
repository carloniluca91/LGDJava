package it.carloni.luca.lgd.steps;

import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import scala.collection.Seq;
import it.carloni.luca.lgd.common.AbstractStep;
import it.carloni.luca.lgd.schemas.CicliLavStep1Schema;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

import static it.carloni.luca.lgd.common.StepUtils.*;


public class CiclilavStep1 extends AbstractStep {

    private final Logger logger = Logger.getLogger(CiclilavStep1.class);

    // required parameters
    private String dataDa;
    private String dataA;

    public CiclilavStep1(String dataDa, String dataA) {

        this.dataDa = dataDa;
        this.dataA = dataA;

        stepInputDir = getValue("ciclilav.step1.input.dir");
        stepOutputDir = getValue("ciclilav.step1.output.dir");

        logger.debug("stepInputDir: " + stepInputDir);
        logger.debug("stepOutputDir: " + stepOutputDir);
        logger.debug("dataDa: " + this.dataDa);
        logger.debug("dataA: " + this.dataA);
    }

    @Override
    public void run(){

        String tlbcidefCsvPath = getValue("ciclilav.step1.tlbcidef.csv");
        String tlbcraccCsvPath = getValue("ciclilav.step1.tlbcracc.csv");
        String ciclilavStep1OutCsv = getValue("ciclilav.step1.out.csv");
        String ciclilavStep1FilecraccCsv = getValue("ciclilav.step1.filecracc.csv");

        logger.debug("tlbcidefCsvPath: " +  tlbcidefCsvPath);
        logger.debug("tlbcraccCsvPath: " + tlbcraccCsvPath);
        logger.debug("ciclilavStep1OutCsv: " + ciclilavStep1OutCsv);
        logger.debug("ciclilavStep1FilecraccCsv: " + ciclilavStep1FilecraccCsv);

        // 22
        Dataset<Row> tlbcidef = readCsvAtPathUsingSchema(tlbcidefCsvPath,
                fromPigSchemaToStructType(CicliLavStep1Schema.getTlbcidefPigSchema()));

        // 40
        // FILTER tlbcidef BY dt_inizio_ciclo >= $data_da AND dt_inizio_ciclo <= $data_a;
        Column dtInizioCicloFilterCol = tlbcidef.col("dt_inizio_ciclo").between(Integer.parseInt(dataDa), Integer.parseInt(dataA));

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

        Dataset<Row> tlbcidefUnpivot = tlbcidef.filter(dtInizioCicloFilterCol).select(
                functions.col("cd_isti"), functions.col("ndg_principale"),
                functions.col("dt_inizio_ciclo"), functions.col("dt_fine_ciclo"),
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
        Dataset<Row> tlbcraccLoad = readCsvAtPathUsingSchema(tlbcraccCsvPath,
                fromPigSchemaToStructType(CicliLavStep1Schema.getTlbcraccLoadPigSchema()));

        // FILTER tlbcracc_load BY data_rif <= ( (int)$data_a <= 20150731 ? 20150731 : (int)$data_a );
        LocalDate defaultDataA = parseStringToLocalDate("20150731", "yyyyMMdd");
        LocalDate dataADate = parseStringToLocalDate(dataA, dataAPattern);
        String greatestDateString = dataADate.compareTo(defaultDataA) <= 0 ?
                defaultDataA.format(DateTimeFormatter.ofPattern("yyyyMMdd")) : dataADate.format(DateTimeFormatter.ofPattern("yyyyMMdd"));

        // FILTER tlbcracc_load BY data_rif <= ( (int)$data_a <= 20150731 ? 20150731 : (int)$data_a );
        Dataset<Row> tlbcracc = tlbcraccLoad.filter(tlbcraccLoad.col("data_rif").leq(Integer.parseInt(greatestDateString)));

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

        // (tlbcracc::cd_isti is not null ? tlbcracc::cd_isti : cicli_racc_1::cd_isti) as cd_isti_ced
        Column cdIstiCedCol = functions.when(tlbcraccClone.col("cd_isti_clone").isNotNull(), tlbcraccClone.col("cd_isti_clone"))
                .otherwise(cicliRacc1.col("cd_isti")).as("cd_isti_ced");

        // (tlbcracc::ndg     is not null ? tlbcracc::ndg     : cicli_racc_1::ndg_principale) as ndg_ced
        Column ndgCedCol = functions.when(tlbcraccClone.col("ndg_clone").isNotNull(), tlbcraccClone.col("ndg_clone"))
                .otherwise(cicliRacc1.col("ndg_principale")).as("ndg_ced");

        Seq<String> joinColsSeq = toScalaStringSeq(Arrays.asList("cod_raccordo", "data_rif"));
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

        writeDatasetAsCsvAtPath(ciclilavStep1, ciclilavStep1OutCsv);
        writeDatasetAsCsvAtPath(ciclilavStep1Filecracc, ciclilavStep1FilecraccCsv);
    }
}
