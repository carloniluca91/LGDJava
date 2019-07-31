package steps.lgdstep;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import steps.abstractstep.AbstractStep;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

public class RaccInc extends AbstractStep {

    private Logger logger;

    public RaccInc(){

        logger = Logger.getLogger(this.getClass().getName());
    }

    @Override
    public void run() {

        String csvFormat = getProperty("csv-format");
        String raccIncInputDir = getProperty("RACC_INC_INPUT_DIR");
        String tlbmignPathCsv = getProperty("TLBMIGN_PATH_CSV");

        logger.info("csvFormat: " + csvFormat);
        logger.info("raccIncInputDir: " + raccIncInputDir);
        logger.info("tlbmignPathCsv: " + tlbmignPathCsv);

        List<String> tlbmignColumnNames = Arrays.asList("cd_isti_ced", "ndg_ced", "cd_abi_ced", "cd_isti_ric", "ndg_ric",
                "cd_abi_ric", "fl01_anag", "fl02_anag", "fl03_anag", "fl04_anag", "fl05_anag", "fl06_anag", "fl07_anag",
                "fl08_anag", "fl09_anag", "fl10_anag", "cod_migraz", "data_migraz", "data_ini_appl", "data_fin_appl",
                "data_ini_appl2");

        StructType tlbmignSchema = getDfSchema(tlbmignColumnNames);
        Dataset<Row> tlbmign = sparkSession.read().format(csvFormat).option("delimiter", ",").schema(tlbmignSchema).csv(
                Paths.get(raccIncInputDir, tlbmignPathCsv).toString());

        // AddDuration(ToDate(data_migraz,'yyyyMMdd'),'P1M') AS month_up
        Column monthUpCol = functions.date_format(functions.add_months(castToDateCol(
                tlbmign.col("data_migraz"), "yyyyMMdd", "yyyy-MM-dd"),
                1), "yyyyMMdd").as("month_up");

        List<Column> tlbmignSelectList = new ArrayList<>();
        tlbmignSelectList.add(tlbmign.col("cd_isti_ric").as("ist_ric_inc"));
        tlbmignSelectList.add(tlbmign.col("ndg_ric").as("ndg_ric_inc"));
        tlbmignSelectList.add(functions.lit(null).as("num_ric_inc"));
        tlbmignSelectList.add(tlbmign.col("cd_isti_ced").as("ist_ced_inc"));
        tlbmignSelectList.add(tlbmign.col("ndg_ced").as("ndg_ced_inc"));
        tlbmignSelectList.add(functions.lit(null).as("num_ced_inc"));
        tlbmignSelectList.add(monthUpCol);

        Seq<Column> tlbmignSelectSeq = JavaConverters.asScalaIteratorConverter(tlbmignSelectList.iterator()).asScala().toSeq();
        String raccIncOutputDir = getProperty("RACC_INC_OUTPUT_DIR");
        logger.info("raccIncOutputDir: " + raccIncOutputDir);

        tlbmign.select(tlbmignSelectSeq).write().format(csvFormat).option("delimiter", ",").mode(SaveMode.Overwrite).csv(
                raccIncOutputDir);
    }
}
