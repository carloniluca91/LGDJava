package steps.lgdstep;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.StructType;
import scala.collection.Seq;
import steps.abstractstep.AbstractStep;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class RaccSoff extends AbstractStep {

    public RaccSoff(){

        logger = Logger.getLogger(this.getClass().getName());

        stepInputDir = getProperty("RACC_SOFF_INPUT_DIR");
        stepOutputDir = getProperty("RACC_SOFF_OUTPUT_DIR");

        logger.info("stepInputDir: " + stepInputDir);
        logger.info("stepOutputDir: " + stepOutputDir);
    }

    @Override
    public void run() {

        String csvFormat = getProperty("csv-format");
        String dblabCsvPath = getProperty("DBLABTLBXD9_PATH_CSV");

        logger.info("csvFormat: " + csvFormat);
        logger.info("dblabCsvPath: " + dblabCsvPath);

        List<String> dllabColumnNames = Arrays.asList(
                "istricsof", "ndgricsof", "numricsof", "istcedsof", "ndgcedsof", "numcedsof", "data_primo_fine_me");
        StructType dllabSchema = getDfSchema(dllabColumnNames);
        Dataset<Row> dllab = sparkSession.read().format(csvFormat).option("delimiter", ",").schema(dllabSchema).csv(
                Paths.get(stepInputDir, dblabCsvPath).toString());

        Map<String, String> columnMap = new HashMap<>();
        columnMap.put("istricsof", "IST_RIC_SOF");
        columnMap.put("ndgricsof", "NDG_RIC_SOF");
        columnMap.put("numricsof", "NUM_RIC_SOF");
        columnMap.put("istcedsof", "IST_CED_SOF");
        columnMap.put("ndgcedsof", "NDG_CED_SOF");
        columnMap.put("numcedsof", "NUM_CED_SOF");
        columnMap.put("data_primo_fine_me", "DATA_FINE_PRIMO_MESE_RIC");

        List<Column> dllabSelectList = selectDfColumns(dllab, columnMap);
        Seq<Column> dllabSelectSeq = toScalaColSeq(dllabSelectList);
        dllab.select(dllabSelectSeq).write().format(csvFormat).option("delimiter", ",").mode(SaveMode.Overwrite)
                .csv(stepOutputDir);

    }
}
