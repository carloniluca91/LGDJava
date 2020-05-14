package it.carloni.luca.lgd.spark.common;

import it.carloni.luca.lgd.parameter.common.AbstractStepValue;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import it.carloni.luca.lgd.spark.udf.UDFFactory;
import it.carloni.luca.lgd.spark.udf.UDFName;

import java.util.Map;

public abstract class AbstractStep<T extends AbstractStepValue> {

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

            logger.error("Could not load application .properties file");
            logger.error(e);
        }
    }

    public String getValue(String key) {
        return properties.getString(key);
    }

    private SparkSession getSparkSessionWithUDFs(){

        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .getOrCreate();

        logger.info("Spark application UI url @ " + sparkSession.sparkContext().uiWebUrl().get());
        return registerUDFs(sparkSession);
    }

    private SparkSession registerUDFs(SparkSession sparkSession){

        sparkSession.udf().register(UDFName.ADD_DURATION.getName(), UDFFactory.buildAddDurationUDF(), DataTypes.StringType);
        sparkSession.udf().register(UDFName.SUBTRACT_DURATION.getName(), UDFFactory.buildSubstractDurationUDF(), DataTypes.StringType);
        sparkSession.udf().register(UDFName.CHANGE_DATE_FORMAT.getName(), UDFFactory.buildChangeDateFormatUDF(), DataTypes.StringType);
        sparkSession.udf().register(UDFName.DAYS_BETWEEN.getName(), UDFFactory.buildDaysBetweenUDF(), DataTypes.LongType);
        sparkSession.udf().register(UDFName.GREATEST_DATE.getName(), UDFFactory.buildGreatestDateUDF(), DataTypes.StringType);
        sparkSession.udf().register(UDFName.LEAST_DATE.getName(), UDFFactory.buildLeastDateUDF(), DataTypes.StringType);

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

        StructField[] structFields = pigSchema.entrySet()
                .stream()
                .map(entry -> {

                    String columnName = entry.getKey();
                    DataType dataType = resolveDataType(entry.getValue());
                    return new StructField(columnName, dataType, true, Metadata.empty());

                })
                .toArray(StructField[]::new);

        return new StructType(structFields);
    }

    private DataType resolveDataType(String pigColumnType){

        switch (pigColumnType){

            case "int": return DataTypes.IntegerType;
            case "double": return DataTypes.DoubleType;
            default: return DataTypes.StringType;
        }
    }

    public abstract void run(T t);
}
