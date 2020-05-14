package it.carloni.luca.lgd.spark.step;

import it.carloni.luca.lgd.parameter.step.UfficioValue;
import it.carloni.luca.lgd.schema.QuadFcollCicliSchema;
import it.carloni.luca.lgd.spark.common.AbstractStep;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;


public class QuadFcollCicli extends AbstractStep<UfficioValue> {

    private final Logger logger = Logger.getLogger(getClass());

    @Override
    public void run(UfficioValue ufficioValue) {

        String ufficio = ufficioValue.getUfficio();

        logger.info(ufficioValue);

        String fcollCsv = getValue("quad.fcoll.cicli.fcoll.csv");
        String cicliNdgLoadCsv = getValue("quad.fcoll.cicli.cicli.ndg.load.csv");
        String fileOutPath = getValue("quad.fcoll.cicli.file.out");

        logger.info("quad.fcoll.cicli.fcoll.csv: " + fcollCsv);
        logger.info("quad.fcoll.cicli.cicli.ndg.load.csv: " + cicliNdgLoadCsv);
        logger.info("quad.fcoll.cicli.file.out: " + fileOutPath);

        Dataset<Row> fcoll = readCsvAtPathUsingSchema(fcollCsv, QuadFcollCicliSchema.getFcollPigSchema());

        Dataset<Row> cicliNdgLoad = readCsvAtPathUsingSchema(cicliNdgLoadCsv, QuadFcollCicliSchema.getCicliNdgLoadPigSchema());

        // JOIN cicli_ndg_load BY (cd_isti_coll, ndg_coll) FULL OUTER,
        //      fcoll          BY (istituto_collegato, ndg_collegato);

        Column joinCondition = cicliNdgLoad.col("cd_isti_coll").equalTo(fcoll.col("istituto_collegato"))
                .and(cicliNdgLoad.col("ndg_coll").equalTo(fcoll.col("ndg_collegato")));

        Column ufficioCol = functions.lit(ufficio).alias("ufficio");

        Dataset<Row> fileOut = cicliNdgLoad.join(fcoll, joinCondition, "full_outer")
                .select(ufficioCol, cicliNdgLoad.col("cd_isti"), cicliNdgLoad.col("ndg_principale"),
                        cicliNdgLoad.col("dt_inizio_ciclo"), cicliNdgLoad.col("cd_isti_coll"),
                        cicliNdgLoad.col("ndg_coll"), fcoll.col("codicebanca").as("fcoll_codicebanca"),
                        fcoll.col("ndgprincipale").as("fcoll_ndgprincipale"),
                        fcoll.col("istituto_collegato").as("fcoll_cd_istituto_coll"),
                        fcoll.col("ndg_collegato").as("fcoll_ndg_coll"),
                        fcoll.col("datainiziodef").as("fcoll_data_inizio_def"))
                .distinct();

        writeDatasetAsCsvAtPath(fileOut, fileOutPath);
    }
}
