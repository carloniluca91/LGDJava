package steps;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.spark.sql.SparkSession;

abstract class AbstractStep extends StepUtils implements StepInterface{

    // initialize logger and sparSession
    final Logger logger = Logger.getLogger(AbstractStep.class.getName());
    final static SparkSession sparkSession = new SparkSession.Builder()
            .appName("LGDApp").master("local").getOrCreate();


    private static Properties configProperties = new Properties();
    private static final String CONFIG_FILE_PATH = "src/main/resources/config.properties";

    AbstractStep(){

        try{
            configProperties = new Properties();
            InputStream inputConfigFile = new FileInputStream(CONFIG_FILE_PATH);
            configProperties.load(inputConfigFile);
        }
        catch (IOException ex){
            ex.printStackTrace();
        }

    }

    String getProperty(String key) {
        return configProperties.getProperty(key);
    }

}
