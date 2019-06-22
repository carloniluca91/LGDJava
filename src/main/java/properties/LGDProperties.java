package properties;

import java.io.InputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;


public class LGDProperties extends Properties {

    private static Properties configProperties = new Properties();
    private static final String CONFIG_FILE_PATH = "src/main/resources/config.properties";

    protected LGDProperties(){

        try{
            configProperties = new Properties();
            InputStream inputConfigFile = new FileInputStream(CONFIG_FILE_PATH);
            configProperties.load(inputConfigFile);
        }
        catch (IOException ex){
            ex.printStackTrace();
        }
    }

    @Override
    public String getProperty(String key) {
        return configProperties.getProperty(key);
    }

}
