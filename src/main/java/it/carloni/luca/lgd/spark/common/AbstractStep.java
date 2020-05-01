package it.carloni.luca.lgd.spark.common;

import it.carloni.luca.lgd.parameter.common.AbstractStepValues;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import it.carloni.luca.lgd.spark.udf.UDFsFactory;
import it.carloni.luca.lgd.spark.udf.UDFsNames;

import java.util.Map;

public abstract class AbstractStep<T extends AbstractStepValues> implements StepInterface<T> {

    private final Logger logger = Logger.getLogger(getClass());
    private final SparkSession sparkSession = getSparkSessionWithUDFs();
    private final PropertiesConfiguration properties = new PropertiesConfiguration();

    private String csvDelimiter;
    private String csvFormat;
    protected String dataDaPattern;
    protected String dataAPattern;

    protected AbstractStep(){

        try {

            properties.load(AbstractStep.class.getClassLoader().getResourceAsStream("lgd.properties"));

            csvFormat = getValue("csv.format");
            csvDelimiter = getValue("csv.delimiter");
            dataDaPattern = getValue("params.datada.pattern");
            dataAPattern = getValue("params.dataa.pattern");

            logger.info("csv.format: " + csvFormat);
            logger.info("csv.delimiter: " + csvDelimiter);
            logger.info("params.datada.pattern: " + dataDaPattern);
            logger.info("params.dataa.pattern: " + dataAPattern);
        }
        catch (ConfigurationException e){

            logger.error("ConfigurationException occurred");
            logger.error(e);
        }
    }

    public String getValue(String key) {
        return properties.getString(key);
    }

    private SparkSession getSparkSessionWithUDFs(){

        SparkSession sparkSession = SparkSession.builder()
                //.master("local")
                .getOrCreate();

        logger.info("Spark application UI url @ " + sparkSession.sparkContext().uiWebUrl().get());
        return registerUDFs(sparkSession);
    }

    private SparkSession registerUDFs(SparkSession sparkSession){

        sparkSession.udf().register(UDFsNames.ADD_DURATION_UDF_NAME, UDFsFactory.addDurationUDF(), DataTypes.StringType);
        sparkSession.udf().register(UDFsNames.SUBTRACT_DURATION_UDF_NAME, UDFsFactory.substractDurationUDF(), DataTypes.StringType);
        sparkSession.udf().register(UDFsNames.CHANGE_DATE_FORMAT_UDF_NAME, UDFsFactory.changeDateFormatUDF(), DataTypes.StringType);
        sparkSession.udf().register(UDFsNames.DAYS_BETWEEN_UDF_NAME, UDFsFactory.daysBetweenUDF(), DataTypes.LongType);
        sparkSession.udf().register(UDFsNames.GREATEST_DATE_UDF_NAME, UDFsFactory.greatestDateUDF(), DataTypes.StringType);
        sparkSession.udf().register(UDFsNames.IS_DATE_BETWEEN_UDF_NAME, UDFsFactory.isDateBetweenLowerDateAndUpperDateUDF(), DataTypes.BooleanType);
        sparkSession.udf().register(UDFsNames.LEAST_DATE_UDF_NAME, UDFsFactory.leastDateUDF(), DataTypes.StringType);

        return sparkSession;
    }

    protected Dataset<Row> readCsvAtPathUsingSchema(String csvFilePath, Map<String, String> pigSchema){

        logger.info("Starting to read data from path " + csvFilePath);

        StructType csvStructType = fromPigSchemaToStructType(pigSchema);
        Dataset<Row> csvDataframe = sparkSession.read()
                .format(csvFormat)
                .option("sep", csvDelimiter)
                .schema(csvStructType)
                .csv(csvFilePath);

        logger.info("Successfully loaded data from path " + csvFilePath);
        return csvDataframe;
    }

    protected void writeDatasetAsCsvAtPath(Dataset<Row> dataset, String path){

        logger.info("Starting to write data at path " + path);

        dataset.coalesce(1)
                .write().
                format(csvFormat)
                .option("sep", csvDelimiter)
                .option("header", true)
                .mode(SaveMode.Overwrite)
                .csv(path);

        logger.info("Successfully written data at path " + path);
    }

    private StructType fromPigSchemaToStructType(Map<String, String> pigSchema){

        StructType schema = new StructType();
        for (Map.Entry<String, String> pigSchemaEntry : pigSchema.entrySet()){

            String columnName = pigSchemaEntry.getKey();
            DataType dataType = resolveDataType(pigSchemaEntry.getValue());
            schema = schema.add(new StructField(columnName, dataType, true, Metadata.empty()));
        }

        return schema;
    }

    private DataType resolveDataType(String pigColumnType){

        switch (pigColumnType){

            case "int": return DataTypes.IntegerType;
            case "double": return DataTypes.DoubleType;
            default: return DataTypes.StringType;
        }
    }
}
