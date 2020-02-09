package steps.lgdstep;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import scala.collection.Seq;
import steps.abstractstep.AbstractStep;
import steps.schemas.RaccSoffSchema;

import java.util.LinkedHashMap;
import java.util.Map;

import static steps.abstractstep.StepUtils.*;

public class RaccSoff extends AbstractStep {

    public RaccSoff(){

        logger = Logger.getLogger(RaccSoff.class);

        stepInputDir = getLGDPropertyValue("racc.soff.input.dir");
        stepOutputDir = getLGDPropertyValue("racc.soff.output.dir");

        logger.debug("stepInputDir: " + stepInputDir);
        logger.debug("stepOutputDir: " + stepOutputDir);
    }

    @Override
    public void run() {

        String dblabCsvPath = getLGDPropertyValue("racc.soff.dblabtlbxd9.path.csv");
        String dllabOutPath = getLGDPropertyValue("racc.soff.dllab");

        logger.debug("dblabCsvPath: " + dblabCsvPath);
        logger.debug("dllabOutPath: " + dllabOutPath);

        Dataset<Row> dllab = sparkSession.read().format(csvFormat).option("delimiter", ",")
                .schema(fromPigSchemaToStructType(RaccSoffSchema.getDblabtlbjxd9PigSchema()))
                .csv(dblabCsvPath);

        Map<String, String> columnMap = new LinkedHashMap<String, String>(){{

            put("istricsof", "IST_RIC_SOF");
            put("ndgricsof", "NDG_RIC_SOF");
            put("numricsof", "NUM_RIC_SOF");
            put("istcedsof", "IST_CED_SOF");
            put("ndgcedsof", "NDG_CED_SOF");
            put("numcedsof", "NUM_CED_SOF");
            put("data_primo_fine_me", "DATA_FINE_PRIMO_MESE_RIC");
        }};

        Seq<Column> dllabSelectSeq = toScalaColSeq(selectDfColumns(dllab, columnMap));
        dllab.select(dllabSelectSeq).write().format(csvFormat).option("delimiter", ",").mode(SaveMode.Overwrite).csv(dllabOutPath);

    }
}
