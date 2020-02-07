package steps.lgdstep;

import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import steps.abstractstep.AbstractStep;
import steps.schemas.QuadFcollSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static steps.abstractstep.StepUtils.*;

public class QuadFcoll extends AbstractStep {

    public QuadFcoll(){

        logger = Logger.getLogger(QuadFcoll.class);

        stepInputDir = getLGDPropertyValue("quad.fcoll.input.dir");
        stepOutputDir = getLGDPropertyValue("quad.fcoll.output.dir");

        logger.debug("stepInputDir: " + stepInputDir);
        logger.debug("stepOutputDir: " + stepOutputDir);
    }

    @Override
    public void run() {

        String fcollCsv = getLGDPropertyValue("quad.fcoll.fcoll.csv");
        String oldFposiLoadCsv = getLGDPropertyValue("quad.fcoll.oldfposi.csv");
        String fileoutdist = getLGDPropertyValue("quad.fcoll.fileoutdist");

        logger.debug("fcollCsv: " + fcollCsv);
        logger.debug("oldFposiLoadCsv: " + oldFposiLoadCsv);
        logger.debug("fileoutdist: " + fileoutdist);

        // 17
        Dataset<Row> fcollLoad = sparkSession.read().format(csvFormat).option("delimiter", ",")
                .schema(fromPigSchemaToStructType(QuadFcollSchema.getFcollLoadPigSchema()))
                .csv(fcollCsv);

        // ToString(ToDate( data_inizio_DEF,'ddMMMyyyy'),'yyyyMMdd')    as data_inizio_DEF
        // ToString(ToDate( data_collegamento,'ddMMMyyyy'),'yyyyMMdd')  as data_collegamento

        Dataset<Row> fcoll = fcollLoad
                .withColumn("data_inizio_DEF", changeDateFormat(fcollLoad.col("data_inizio_DEF"), "ddMMyyyy", "yyyyMMdd"))
                .withColumn("data_collegamento", changeDateFormat(fcollLoad.col("data_collegamento"), "ddMMyyyy", "yyyyMMdd"));

        // 39

        // 44
        Dataset<Row> oldFposiLoad = sparkSession.read().format(csvFormat).option("delimiter", ",")
                .schema(fromPigSchemaToStructType(QuadFcollSchema.getOldFposiLoadPigSchema()))
                .csv(oldFposiLoadCsv);

        // FILTER oldfposi_load BY dataINIZIOPD is not null OR datainizioinc is not null OR dataSOFFERENZA is not null
        Column filterConditionCol = oldFposiLoad.col("dataINIZIOPD").isNotNull()
                .or(oldFposiLoad.col("datainizioinc").isNotNull())
                .or(oldFposiLoad.col("dataSOFFERENZA").isNotNull());

        // ToString(ToDate( datainizioDEF,'yy-MM-dd'),'yyyyMMdd')   as datainizioDEF
        // ToString(ToDate( dataFINEDEF,'yy-MM-dd'),'yyyyMMdd')   as dataFINEDEF
        Dataset<Row> oldFposi = oldFposiLoad.filter(filterConditionCol)
                .withColumn("datainizioDEF", changeDateFormat(oldFposiLoad.col("datainizioDEF"), "yy-MM-dd", "yyyyMMdd"))
                .withColumn("dataFINEDEF", changeDateFormat(oldFposiLoad.col("dataFINEDEF"), "yy-MM-dd", "yyyyMMdd"));

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

        List<Column> fileOutDistSelectColList = new ArrayList<>(selectDfColumns(
                oldFposi, Arrays.asList("codicebanca", "ndgprincipale", "datainizioDEF", "dataFINEDEF")));

        fileOutDistSelectColList.add(dataDefaultCol);
        fileOutDistSelectColList.add(istitutoCollegatoCol);
        fileOutDistSelectColList.add(ndgCollegatoCol);
        fileOutDistSelectColList.add(dataCollegamentoCol);
        fileOutDistSelectColList.add(fcoll.col("cumulo"));

        // JOIN oldfposi BY (cumulo) LEFT, fcoll BY (cumulo);
        Dataset<Row> fileOutDist = oldFposi.join(fcoll, oldFposi.col("cumulo").equalTo(fcoll.col("cumulo")), "left")
                .select(toScalaColSeq(fileOutDistSelectColList)).distinct();

        fileOutDist.write().format(csvFormat).option("delimiter", ",").mode(SaveMode.Overwrite).csv(fileoutdist);
    }
}
