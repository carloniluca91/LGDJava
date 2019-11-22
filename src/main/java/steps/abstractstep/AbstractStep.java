package steps.abstractstep;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

public abstract class AbstractStep extends StepUtils implements StepInterface {

    protected Logger logger;
    protected static SparkSession sparkSession;
    private static Properties configProperties;
    private static final String configFilePath = "src/main/resources/lgd.properties";

    protected String dataInputDir;
    protected String dataOutputDir;

    // input and output dirs for a step
    protected String stepInputDir;
    protected String stepOutputDir;

    protected AbstractStep(String loggerName){

        logger = Logger.getLogger(loggerName);
        initializeSparkSessionWithUDFs();

        try{

            configProperties = new Properties();
            InputStream inputConfigFile = new FileInputStream(configFilePath);
            configProperties.load(inputConfigFile);

            dataInputDir = getPropertyValue("data.input.dir");
            dataOutputDir = getPropertyValue("data.output.dir");

            logger.debug("dataInputDir: " + dataInputDir);
            logger.debug("dataOutputDir: " + dataOutputDir);
        }
        catch (IOException ex){

            logger.error(ex.getMessage());
        }
    }

    public String getPropertyValue(String key) {
        return configProperties.getProperty(key);
    }

    private void initializeSparkSessionWithUDFs(){

        sparkSession = new SparkSession.Builder()
                .appName("LGDApp")
                .master("local")
                .getOrCreate();

    }

    private void registerUDFs(){

        // sparkSession.udf().register();
    }
}
