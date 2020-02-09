package steps.lgdstep;

import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.Seq;
import steps.abstractstep.AbstractStep;
import steps.schemas.RaccIncSchema;

import java.util.ArrayList;
import java.util.List;

import static steps.abstractstep.StepUtils.*;

public class RaccInc extends AbstractStep {

    public RaccInc(){

        logger = Logger.getLogger(RaccInc.class);

        stepInputDir = getLGDPropertyValue("racc.inc.input.dir");
        stepOutputDir = getLGDPropertyValue("racc.inc.output.dir");

        logger.debug("stepInputDir: " + stepInputDir);
        logger.debug("stepOutputDir: " + stepOutputDir);
    }

    @Override
    public void run() {

        String tlbmignPathCsv = getLGDPropertyValue("racc.inc.tlbmign.path.csv");
        String raccIncOutPath = getLGDPropertyValue("racc.inc.racc.inc.out");

        logger.debug("tlbmignPathCsv: " + tlbmignPathCsv);
        logger.debug("raccIncOutPath: " + raccIncOutPath);

        Dataset<Row> tlbmign = sparkSession.read().format(csvFormat).option("delimiter", ",")
                .schema(fromPigSchemaToStructType(RaccIncSchema.getTlbmignPigSchema()))
                .csv(tlbmignPathCsv);

        // AddDuration(ToDate(data_migraz,'yyyyMMdd'),'P1M') AS month_up
        Column monthUpCol = addDuration(tlbmign.col("data_migraz"), "yyyyMMdd", 1);

        List<Column> tlbmignSelectList = new ArrayList<>();
        tlbmignSelectList.add(tlbmign.col("cd_isti_ric").as("ist_ric_inc"));
        tlbmignSelectList.add(tlbmign.col("ndg_ric").as("ndg_ric_inc"));
        tlbmignSelectList.add(functions.lit(null).cast(DataTypes.StringType).as("num_ric_inc"));
        tlbmignSelectList.add(tlbmign.col("cd_isti_ced").as("ist_ced_inc"));
        tlbmignSelectList.add(tlbmign.col("ndg_ced").as("ndg_ced_inc"));
        tlbmignSelectList.add(functions.lit(null).cast(DataTypes.StringType).as("num_ced_inc"));
        tlbmignSelectList.add(tlbmign.col("data_migraz"));

        Seq<Column> tlbmignSelectSeq = toScalaColSeq(tlbmignSelectList);
        Dataset<Row> raccIncOut = tlbmign.select(tlbmignSelectSeq)
                .withColumn("month_up", monthUpCol)
                .drop("data_migraz");

        raccIncOut.write().format(csvFormat).option("delimiter", ",").mode(SaveMode.Overwrite).csv(raccIncOutPath);
    }
}
