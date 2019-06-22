package steps;

import java.util.Properties;
import properties.LGDProperties;
import org.apache.spark.sql.SparkSession;

public abstract class AbstractStep extends LGDProperties {

    private Properties stepParameters = new Properties();      // parameters for the class
    public final static SparkSession sparkSession = new SparkSession.Builder()
            .appName("LGDApp").master("local").getOrCreate();

    AbstractStep(String[] parameters){

        // retrieve step parameters by means of LGDProperties
        for (String parameter: parameters){
            stepParameters.setProperty(parameter, super.getProperty(parameter));
        }
    }

    @Override
    public String getProperty(String key) {
        return stepParameters.getProperty(key);
    }

}
