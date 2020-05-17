package it.carloni.luca.lgd.spark.step;

import it.carloni.luca.lgd.parameter.step.EmptyValue;
import it.carloni.luca.lgd.schema.QuadFcollSchema;
import it.carloni.luca.lgd.spark.common.AbstractStep;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import static it.carloni.luca.lgd.spark.utils.StepUtils.changeDateFormatFromY2toY4UDF;
import static it.carloni.luca.lgd.spark.utils.StepUtils.changeDateFormatUDF;

public class QuadFcoll extends AbstractStep<EmptyValue> {

    private final Logger logger = Logger.getLogger(getClass());

    @Override
    public void run(EmptyValue emptyValue) {

        String fcollCsv = getValue("quad.fcoll.fcoll.csv");
        String oldFposiLoadCsv = getValue("quad.fcoll.oldfposi.csv");
        String fileoutdist = getValue("quad.fcoll.fileoutdist");

        logger.info("quad.fcoll.fcoll.csv: " + fcollCsv);
        logger.info("quad.fcoll.oldfposi.csv: " + oldFposiLoadCsv);
        logger.info("quad.fcoll.fileoutdist: " + fileoutdist);

        // 17
        Dataset<Row> fcollLoad = readCsvAtPathUsingSchema(fcollCsv, QuadFcollSchema.getFcollLoadPigSchema());

        // ToString(ToDate( data_inizio_DEF,'ddMMyyyy'),'yyyyMMdd')    as data_inizio_DEF
        // ToString(ToDate( data_collegamento,'ddMMyyyy'),'yyyyMMdd')  as data_collegamento

        String oldPattern = "ddMMyyyy";
        String newPattern = "yyyyMMdd";

        Dataset<Row> fcoll = fcollLoad
                .withColumn("data_inizio_DEF", changeDateFormatUDF(functions.col("data_inizio_DEF"), oldPattern, newPattern))
                .withColumn("data_collegamento", changeDateFormatUDF(functions.col("data_collegamento"), oldPattern, newPattern));

        // 39

        // 44
        Dataset<Row> oldFposiLoad = readCsvAtPathUsingSchema(oldFposiLoadCsv, QuadFcollSchema.getOldFposiLoadPigSchema());

        // FILTER oldfposi_load BY dataINIZIOPD is not null OR datainizioinc is not null OR dataSOFFERENZA is not null
        Column filterConditionCol = oldFposiLoad.col("dataINIZIOPD").isNotNull()
                .or(oldFposiLoad.col("datainizioinc").isNotNull())
                .or(oldFposiLoad.col("dataSOFFERENZA").isNotNull());

        // ToString(ToDate( datainizioDEF,'yy-MM-dd'),'yyyyMMdd')   as datainizioDEF
        // ToString(ToDate( dataFINEDEF,'yy-MM-dd'),'yyyyMMdd')   as dataFINEDEF
        Dataset<Row> oldFposi = oldFposiLoad.filter(filterConditionCol)
                .withColumn("datainizioDEF", changeDateFormatFromY2toY4UDF(functions.col("datainizioDEF"), "yy-MM-dd", newPattern))
                .withColumn("dataFINEDEF", changeDateFormatFromY2toY4UDF(functions.col("dataFINEDEF"), "yy-MM-dd", newPattern));

        // 76

        // 80

        // ( fcoll::data_inizio_DEF   is null ? oldfposi::datainizioDEF : fcoll::data_inizio_DEF )     as DATA_DEFAULT
        Column dataDefaultCol = functions.when(fcoll.col("data_inizio_DEF").isNull(), oldFposi.col("datainizioDEF"))
                .otherwise(fcoll.col("data_inizio_DEF")).alias("DATA_DEFAULT");

        // ( fcoll::cd_istituto_COLL  is null ? oldfposi::codicebanca   : fcoll::cd_istituto_COLL )    as ISTITUTO_COLLEGATO
        Column istitutoCollegatoCol = functions.when(fcoll.col("cd_istituto_COLL").isNull(), oldFposi.col("codicebanca"))
                .otherwise(fcoll.col("cd_istituto_COLL")).alias("ISTITUTO_COLLEGATO");

        // ( fcoll::ndg_COLL          is null ? oldfposi::ndgprincipale : fcoll::ndg_COLL )            as NDG_COLLEGATO
        Column ndgCollegatoCol = functions.when(fcoll.col("ndg_COLL").isNull(), oldFposi.col("ndgprincipale"))
                .otherwise(fcoll.col("ndg_COLL")).alias("NDG_COLLEGATO");

        // ( fcoll::data_collegamento is null ? oldfposi::datainizioDEF : fcoll::data_collegamento )   as DATA_COLLEGAMENTO
        Column dataCollegamentoCol = functions.when(fcoll.col("data_collegamento").isNull(), oldFposi.col("datainizioDEF"))
                .otherwise(fcoll.col("data_collegamento")).alias("DATA_COLLEGAMENTO");

        // JOIN oldfposi BY (cumulo) LEFT, fcoll BY (cumulo);
        Dataset<Row> fileOutDist = oldFposi.join(fcoll, oldFposi.col("cumulo").equalTo(fcoll.col("cumulo")), "left")
                .select(oldFposi.col("codicebanca"), oldFposi.col("ndgprincipale"), oldFposi.col("datainizioDEF"),
                        oldFposi.col("dataFINEDEF"), dataDefaultCol, istitutoCollegatoCol, ndgCollegatoCol, dataCollegamentoCol,
                        fcoll.col("cumulo"))
                .distinct();

        writeDatasetAsCsvAtPath(fileOutDist, fileoutdist);
    }
}
