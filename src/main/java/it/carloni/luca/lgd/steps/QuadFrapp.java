package it.carloni.luca.lgd.steps;

import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import it.carloni.luca.lgd.common.AbstractStep;
import it.carloni.luca.lgd.schemas.QuadFrappSchema;

import java.util.Arrays;
import java.util.List;

import static it.carloni.luca.lgd.common.StepUtils.*;

public class QuadFrapp extends AbstractStep {

    private final Logger logger = Logger.getLogger(QuadFrapp.class);

    private String ufficio;

    public QuadFrapp(String ufficio){

        this.ufficio = ufficio;

        logger.debug("ufficio: " + this.ufficio);
    }

    @Override
    public void run() {

        String hadoopFrappCsv = getValue("quad.frapp.hadoop.frapp.csv");
        String oldFrappLoadCsv = getValue("quad.frapp.old.frapp.load.csv");
        String fcollCsv = getValue("quad.frapp.fcoll.csv");
        String hadoopFrappOutPath = getValue("quad.frapp.hadoop.frapp.out");
        String oldFrappOutPath = getValue("quad.frapp.old.frapp.out");

        logger.debug("hadoopFrappCsv: " + hadoopFrappCsv);
        logger.debug("oldFrappLoadCsv: " + oldFrappLoadCsv);
        logger.debug("fcollCsv: " + fcollCsv);
        logger.debug("hadoopFrappOutPath: " + hadoopFrappOutPath);
        logger.debug("oldFrappOutPath: " + oldFrappOutPath);

        Dataset<Row> hadoopFrapp = readCsvAtPathUsingSchema(hadoopFrappCsv, fromPigSchemaToStructType(QuadFrappSchema.getHadoopFrappPigSchema()));
        Dataset<Row> oldFrappLoad = readCsvAtPathUsingSchema(oldFrappLoadCsv, fromPigSchemaToStructType(QuadFrappSchema.getOldFrappLoadPigSchema()));
        Dataset<Row> fcoll = readCsvAtPathUsingSchema(fcollCsv, fromPigSchemaToStructType(QuadFrappSchema.getFcollPigSchema()));

        // JOIN oldfrapp_load BY (CODICEBANCA, NDG), fcoll BY (ISTITUTO_COLLEGATO, NDG_COLLEGATO);

        Column oldFrappJoinCondition = oldFrappLoad.col("CODICEBANCA").equalTo(fcoll.col("ISTITUTO_COLLEGATO"))
                .and(oldFrappLoad.col("NDG").equalTo(fcoll.col("NDG_COLLEGATO")));

        // FILTER
        // BY ToDate(oldfrapp_load::DT_RIFERIMENTO,'yyyyMMdd') >= ToDate( fcoll::DATAINIZIODEF,'yyyyMMdd')
        // AND ToDate(oldfrapp_load::DT_RIFERIMENTO,'yyyyMMdd') <= ToDate( fcoll::DATAFINEDEF,'yyyyMMdd'  )

        Column filterCondition = isDateBetween(oldFrappLoad.col("DT_RIFERIMENTO"), "yyyyMMdd",
                fcoll.col("DATAINIZIODEF"), "yyyyMMdd",
                fcoll.col("DATAFINEDEF"), "yyyyMMdd");

        Dataset<Row> oldFrapp = oldFrappLoad.join(fcoll, oldFrappJoinCondition)
                .filter(filterCondition)
                .select(oldFrappLoad.col("*"),
                        fcoll.col("CODICEBANCA").alias("CODICEBANCA_PRINC"),
                        fcoll.col("NDGPRINCIPALE"),
                        fcoll.col("DATAINIZIODEF"));

        // JOIN hadoop_frapp BY (codicebanca_princ, ndgprincipale, datainiziodef, codicebanca, ndg, sportello, conto, datariferimento) FULL OUTER,
        // oldfrapp BY (CODICEBANCA_PRINC, NDGPRINCIPALE, DATAINIZIODEF, CODICEBANCA, NDG, SPORTELLO, CONTO, DT_RIFERIMENTO);

        List<String> joinColumnNames = Arrays.asList("codicebanca_princ", "ndgprincipale", "datainiziodef", "codicebanca", "ndg", "sportello", "conto");
        Column hadoopFrappOutJoinCondition = getQuadJoinCondition(hadoopFrapp, oldFrapp, joinColumnNames)
                .and(hadoopFrapp.col("datariferimento").equalTo(oldFrapp.col("DT_RIFERIMENTO")));

        // FILTER hadoop_frapp_oldfrapp_join BY oldfrapp::CODICEBANCA IS NULL;

        Dataset<Row> hadoopFrappOut = hadoopFrapp.join(oldFrapp, hadoopFrappOutJoinCondition, "full_outer")
                .filter(oldFrapp.col("CODICEBANCA").isNull())
                .select(functions.lit(ufficio).alias("ufficio"), hadoopFrapp.col("*"), oldFrapp.col("*"));

        Dataset<Row> oldFrappOut = hadoopFrapp.join(oldFrapp, hadoopFrappOutJoinCondition, "full_outer")
                .filter(hadoopFrapp.col("codicebanca").isNull())
                .select(functions.lit(ufficio).alias("ufficio"), hadoopFrapp.col("*"), oldFrapp.col("*"));

        writeDatasetAsCsvAtPath(hadoopFrappOut, hadoopFrappOutPath);
        writeDatasetAsCsvAtPath(oldFrappOut, oldFrappOutPath);

    }

    private Column getQuadJoinCondition(Dataset<Row> datasetLeft, Dataset<Row> datasetRight, List<String> joinColumnNames){

        Column joinCondition = datasetLeft.col(joinColumnNames.get(0))
                .equalTo(datasetRight.col(joinColumnNames.get(0).toUpperCase()));

        for (String joinColumnName: joinColumnNames.subList(1, joinColumnNames.toArray().length - 1)){

            joinCondition = joinCondition.and(datasetLeft.col(joinColumnName)
                    .equalTo(datasetRight.col(joinColumnName.toUpperCase())));
        }

        return joinCondition;
    }
}
