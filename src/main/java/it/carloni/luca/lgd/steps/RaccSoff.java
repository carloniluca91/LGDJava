package it.carloni.luca.lgd.steps;

import it.carloni.luca.lgd.common.StepUtils;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.collection.Seq;
import it.carloni.luca.lgd.common.AbstractStep;
import it.carloni.luca.lgd.schemas.RaccSoffSchema;

import java.util.LinkedHashMap;
import java.util.Map;

import static it.carloni.luca.lgd.common.StepUtils.*;

public class RaccSoff extends AbstractStep {

    private final Logger logger = Logger.getLogger(RaccSoff.class);

    public RaccSoff(){

        stepInputDir = getValue("racc.soff.input.dir");
        stepOutputDir = getValue("racc.soff.output.dir");

        logger.debug("stepInputDir: " + stepInputDir);
        logger.debug("stepOutputDir: " + stepOutputDir);
    }

    @Override
    public void run() {

        String dblabCsvPath = getValue("racc.soff.dblabtlbxd9.path.csv");
        String dllabOutPath = getValue("racc.soff.dllab");

        logger.debug("dblabCsvPath: " + dblabCsvPath);
        logger.debug("dllabOutPath: " + dllabOutPath);

        Dataset<Row> dllab = readCsvAtPathUsingSchema(dblabCsvPath, fromPigSchemaToStructType(RaccSoffSchema.getDblabtlbjxd9PigSchema()));
        Map<String, String> columnMap = new LinkedHashMap<String, String>(){{

            put("istricsof", "IST_RIC_SOF");
            put("ndgricsof", "NDG_RIC_SOF");
            put("numricsof", "NUM_RIC_SOF");
            put("istcedsof", "IST_CED_SOF");
            put("ndgcedsof", "NDG_CED_SOF");
            put("numcedsof", "NUM_CED_SOF");
            put("data_primo_fine_me", "DATA_FINE_PRIMO_MESE_RIC");
        }};

        Seq<Column> dllabSelectSeq = StepUtils.toScalaSeq(selectDfColumns(dllab, columnMap));
        writeDatasetAsCsvAtPath(dllab.select(dllabSelectSeq), dllabOutPath);
    }
}
