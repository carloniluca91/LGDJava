package steps.abstractstep;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.spark.sql.SparkSession;

public abstract class AbstractStep extends StepUtils implements StepInterface {

    // logger
    protected Logger logger;

    // sparSession
    protected final static SparkSession sparkSession = new SparkSession.Builder()
            .appName("LGDApp").master("local").getOrCreate();

    // properties
    private static Properties configProperties = new Properties();
    private static final String configFilePath = "src/main/resources/config.properties";

    // input and output dirs
    protected static String stepInputDir;
    protected static String stepOutputDir;

    protected AbstractStep(){

        try{
            configProperties = new Properties();
            InputStream inputConfigFile = new FileInputStream(configFilePath);
            configProperties.load(inputConfigFile);
        }
        catch (IOException ex){
            ex.printStackTrace();
        }

    }

    public String getProperty(String key) {
        return configProperties.getProperty(key);
    }

}
