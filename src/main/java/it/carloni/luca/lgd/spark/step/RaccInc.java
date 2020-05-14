package it.carloni.luca.lgd.spark.step;

import it.carloni.luca.lgd.parameter.step.EmptyValue;
import it.carloni.luca.lgd.schema.RaccIncSchema;
import it.carloni.luca.lgd.spark.common.AbstractStep;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import static it.carloni.luca.lgd.spark.utils.StepUtils.addDurationUDF;
import static it.carloni.luca.lgd.spark.utils.StepUtils.toStringCol;

public class RaccInc extends AbstractStep<EmptyValue> {

    private final Logger logger = Logger.getLogger(RaccInc.class);

    @Override
    public void run(EmptyValue emptyValue) {

        String tlbmignPathCsv = getValue("racc.inc.tlbmign.path.csv");
        String raccIncOutPath = getValue("racc.inc.racc.inc.out");

        logger.info("racc.inc.tlbmign.path.csv: " + tlbmignPathCsv);
        logger.info("racc.inc.racc.inc.out: " + raccIncOutPath);

        Dataset<Row> tlbmign = readCsvAtPathUsingSchema(tlbmignPathCsv, RaccIncSchema.getTlbmignPigSchema());

        // AddDuration(ToDate(data_migraz,'yyyyMMdd'),'P1M') AS month_up
        Column monthUpCol = addDurationUDF(tlbmign.col("data_migraz"), "yyyyMMdd", 1);

        Dataset<Row> raccIncOut = tlbmign
                .select(tlbmign.col("cd_isti_ric").as("ist_ric_inc"), tlbmign.col("ndg_ric").as("ndg_ric_inc"),
                        toStringCol(functions.lit(null)).as("num_ric_inc"), tlbmign.col("cd_isti_ced").as("ist_ced_inc"),
                        tlbmign.col("ndg_ced").as("ndg_ced_inc"), toStringCol(functions.lit(null)).as("num_ced_inc"),
                        tlbmign.col("data_migraz"))
                .withColumn("month_up", monthUpCol)
                .drop("data_migraz");

        writeDatasetAsCsvAtPath(raccIncOut, raccIncOutPath);
    }
}
