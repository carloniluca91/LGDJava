package steps;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

abstract class AbstractStep implements StepInterface{

    // initialize logger and sparSession
    final Logger logger = Logger.getLogger(CicliLavStep1.class.getName());
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

    // create a schema with one String column for each name provided
    StructType setDfSchema(List<String> columnNames){

        StructType schema = new StructType();
        for (String columnName: columnNames){

            schema = schema.add(new StructField(columnName, DataTypes.StringType, true, Metadata.empty()));
            logger.info("added column " + columnName + " to schema");
        }

        return schema;
    }

    Column getUnixTimeStampCol(Dataset<Row> df, String columnName, String dateFormat){
        return functions.unix_timestamp(df.col(columnName), dateFormat);
    }

    Column getUnixTimeStampCol(Column column, String dateFormat){
        return functions.unix_timestamp(column, dateFormat);
    }

    Column castCol(Dataset<Row> df, String columnName, DataType dataType){
        return df.col(columnName).cast(dataType);
    }

    Column castCol(Column column, DataType dataType){
        return column.cast(dataType);
    }

}
