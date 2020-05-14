package it.carloni.luca.lgd.spark.step;

import it.carloni.luca.lgd.parameter.step.UfficioValue;
import it.carloni.luca.lgd.schema.QuadFrappSchema;
import it.carloni.luca.lgd.spark.common.AbstractStep;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.Arrays;

public class QuadFrapp extends AbstractStep<UfficioValue> {

    private final Logger logger = Logger.getLogger(getClass());

    @Override
    public void run(UfficioValue ufficioValue) {

        String ufficio = ufficioValue.getUfficio();

        logger.info(ufficioValue);

        String hadoopFrappCsv = getValue("quad.frapp.hadoop.frapp.csv");
        String oldFrappLoadCsv = getValue("quad.frapp.old.frapp.load.csv");
        String fcollCsv = getValue("quad.frapp.fcoll.csv");
        String hadoopFrappOutPath = getValue("quad.frapp.hadoop.frapp.out");
        String oldFrappOutPath = getValue("quad.frapp.old.frapp.out");

        logger.info("quad.frapp.hadoop.frapp.csv: " + hadoopFrappCsv);
        logger.info("quad.frapp.old.frapp.load.csv: " + oldFrappLoadCsv);
        logger.info("quad.frapp.fcoll.csv: " + fcollCsv);
        logger.info("quad.frapp.hadoop.frapp.out: " + hadoopFrappOutPath);
        logger.info("quad.frapp.old.frapp.out: " + oldFrappOutPath);

        Dataset<Row> hadoopFrapp = readCsvAtPathUsingSchema(hadoopFrappCsv, QuadFrappSchema.getHadoopFrappPigSchema());
        Dataset<Row> oldFrappLoad = readCsvAtPathUsingSchema(oldFrappLoadCsv, QuadFrappSchema.getOldFrappLoadPigSchema());
        Dataset<Row> fcoll = readCsvAtPathUsingSchema(fcollCsv, QuadFrappSchema.getFcollPigSchema());

        // JOIN oldfrapp_load BY (CODICEBANCA, NDG), fcoll BY (ISTITUTO_COLLEGATO, NDG_COLLEGATO);

        Column oldFrappJoinCondition = oldFrappLoad.col("CODICEBANCA").equalTo(fcoll.col("ISTITUTO_COLLEGATO"))
                .and(oldFrappLoad.col("NDG").equalTo(fcoll.col("NDG_COLLEGATO")));

        // FILTER
        // BY ToDate(oldfrapp_load::DT_RIFERIMENTO,'yyyyMMdd') >= ToDate( fcoll::DATAINIZIODEF,'yyyyMMdd')
        // AND ToDate(oldfrapp_load::DT_RIFERIMENTO,'yyyyMMdd') <= ToDate( fcoll::DATAFINEDEF,'yyyyMMdd'  )

        Column filterCondition = oldFrappLoad.col("DT_RIFERIMENTO")
                .between(fcoll.col("DATAINIZIODEF"), fcoll.col("DATAFINEDEF"));

        Dataset<Row> oldFrapp = oldFrappLoad.join(fcoll, oldFrappJoinCondition)
                .filter(filterCondition)
                .select(oldFrappLoad.col("*"),
                        fcoll.col("CODICEBANCA").alias("CODICEBANCA_PRINC"),
                        fcoll.col("NDGPRINCIPALE"),
                        fcoll.col("DATAINIZIODEF"));

        // JOIN hadoop_frapp BY (codicebanca_princ, ndgprincipale, datainiziodef, codicebanca, ndg, sportello, conto, datariferimento) FULL OUTER,
        // oldfrapp BY (CODICEBANCA_PRINC, NDGPRINCIPALE, DATAINIZIODEF, CODICEBANCA, NDG, SPORTELLO, CONTO, DT_RIFERIMENTO);

        String[] columnNames = {"codicebanca_princ", "ndgprincipale", "datainiziodef", "codicebanca", "ndg", "sportello", "conto"};
        Column hadoopFrappOutJoinCondition = getQuadJoinCondition(hadoopFrapp, oldFrapp, columnNames);

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

    private Column getQuadJoinCondition(Dataset<Row> datasetLeft, Dataset<Row> datasetRight, String[] joinColumnNames){

        return Arrays.stream(joinColumnNames)
                .map(columnName -> datasetLeft.col(columnName).equalTo(datasetRight.col(columnName.toUpperCase())))
                .reduce(datasetLeft.col("datariferimento").equalTo(datasetRight.col("DT_RIFERIMENTO")), (Column::and));
    }
}
