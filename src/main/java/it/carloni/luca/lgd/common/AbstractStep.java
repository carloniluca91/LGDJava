package it.carloni.luca.lgd.common;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import it.carloni.luca.lgd.common.udfs.UDFsFactory;
import it.carloni.luca.lgd.common.udfs.UDFsNames;

public abstract class AbstractStep {

    private final PropertiesConfiguration propertiesConfiguration = new PropertiesConfiguration();
    protected SparkSession sparkSession;

    // input and output dirs for a step
    protected String stepInputDir;
    protected String stepOutputDir;

    protected String dataDaPattern;
    protected String dataAPattern;
    protected String csvFormat;
    private String csvDelimiter;

    protected AbstractStep(){

        Logger logger = Logger.getLogger(AbstractStep.class);

        try {

            propertiesConfiguration.load(AbstractStep.class.getClassLoader().getResourceAsStream("lgd.properties"));

            csvFormat = getValue("csv.format");
            csvDelimiter = getValue("csv.delimiter");
            dataDaPattern = getValue("params.datada.pattern");
            dataAPattern = getValue("params.dataa.pattern");

            logger.debug("csvFormat: " + csvFormat);
            logger.debug("csvDelimiter: " + csvDelimiter);
            logger.debug("dataDaPattern: " + dataDaPattern);
            logger.debug("dataAPattern: " + dataAPattern);

            getSparkSessionWithUDFs();
        }
        catch (ConfigurationException ex){

            logger.error("ConfigurationException occurred");
            logger.error("ex.getMessage(): " + ex.getMessage());
            logger.error(ex);
        }
    }

    public String getValue(String key) {
        return propertiesConfiguration.getString(key);
    }

    private void getSparkSessionWithUDFs(){

        sparkSession = SparkSession.builder().getOrCreate();
        registerUDFs(sparkSession);
    }

    private void registerUDFs(SparkSession sparkSession){

        sparkSession.udf().register(UDFsNames.ADD_DURATION_UDF_NAME, UDFsFactory.addDurationUDF(), DataTypes.StringType);
        sparkSession.udf().register(UDFsNames.SUBTRACT_DURATION_UDF_NAME, UDFsFactory.substractDurationUDF(), DataTypes.StringType);
        sparkSession.udf().register(UDFsNames.CHANGE_DATE_FORMAT_UDF_NAME, UDFsFactory.changeDateFormatUDF(), DataTypes.StringType);
        sparkSession.udf().register(UDFsNames.DAYS_BETWEEN_UDF_NAME, UDFsFactory.daysBetweenUDF(), DataTypes.LongType);
        sparkSession.udf().register(UDFsNames.GREATEST_DATE_UDF_NAME, UDFsFactory.greatestDateUDF(), DataTypes.StringType);
        sparkSession.udf().register(UDFsNames.IS_DATE_BETWEEN_UDF_NAME, UDFsFactory.isDateBetweenLowerDateAndUpperDateUDF(), DataTypes.BooleanType);
        sparkSession.udf().register(UDFsNames.LEAST_DATE_UDF_NAME, UDFsFactory.leastDateUDF(), DataTypes.StringType);
    }

    protected Dataset<Row> readCsvAtPathUsingSchema(String csvFilePath, StructType csvSchema){

        return sparkSession.read().format(csvFormat).option("sep", csvDelimiter).schema(csvSchema).csv(csvFilePath);
    }

    protected void writeDatasetAsCsvAtPath(Dataset<Row> dataset, String path){

        dataset.write().format(csvFormat).option("sep", csvDelimiter).mode(SaveMode.Overwrite).csv(path);
    }

    abstract public void run();
}
