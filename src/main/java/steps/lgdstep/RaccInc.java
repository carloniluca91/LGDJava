package steps.lgdstep;

import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.Seq;
import steps.abstractstep.AbstractStep;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RaccInc extends AbstractStep {

    public RaccInc(String loggerName){

        super(loggerName);
        logger = Logger.getLogger(loggerName);

        stepInputDir = getLGDPropertyValue("racc.inc.input.dir");
        stepOutputDir = getLGDPropertyValue("racc.inc.output.dir");

        logger.debug("stepInputDir: " + stepInputDir);
        logger.debug("stepOutputDir: " + stepOutputDir);
    }

    @Override
    public void run() {

        String csvFormat = getLGDPropertyValue("csv.format");
        String tlbmignPathCsv = getLGDPropertyValue("tlbmign.path.csv");

        logger.debug("csvFormat: " + csvFormat);
        logger.debug("tlbmignPathCsv: " + tlbmignPathCsv);

        List<String> tlbmignColumnNames = Arrays.asList("cd_isti_ced", "ndg_ced", "cd_abi_ced", "cd_isti_ric", "ndg_ric",
                "cd_abi_ric", "fl01_anag", "fl02_anag", "fl03_anag", "fl04_anag", "fl05_anag", "fl06_anag", "fl07_anag",
                "fl08_anag", "fl09_anag", "fl10_anag", "cod_migraz", "data_migraz", "data_ini_appl", "data_fin_appl",
                "data_ini_appl2");

        Dataset<Row> tlbmign = sparkSession.read().format(csvFormat).option("delimiter", ",")
                .schema(getStringTypeSchema(tlbmignColumnNames))
                .csv(Paths.get(stepInputDir, tlbmignPathCsv).toString());

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
        Dataset<Row> raccIncOut = tlbmign.select(tlbmignSelectSeq).withColumn("month_up", monthUpCol).drop("data_migraz");

        raccIncOut.write().format(csvFormat).option("delimiter", ",").mode(SaveMode.Overwrite).csv(stepOutputDir);
    }
}
