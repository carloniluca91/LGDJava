package steps.abstractstep;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import steps.abstractstep.udfs.UDFsFactory;

public abstract class AbstractStep {

    protected Logger logger;
    protected SparkSession sparkSession;
    private PropertiesConfiguration propertiesConfiguration;

    // input and output dirs for a step
    protected String stepInputDir;
    protected String stepOutputDir;

    protected String dataDaPattern;
    protected String dataAPattern;
    protected String csvFormat;

    protected AbstractStep(){

        logger = Logger.getLogger(AbstractStep.class);

        try {

            propertiesConfiguration = new PropertiesConfiguration();
            propertiesConfiguration.load(AbstractStep.class.getClassLoader().getResourceAsStream("lgd.properties"));

            csvFormat = getLGDPropertyValue("csv.format");
            dataDaPattern = getLGDPropertyValue("params.datada.pattern");
            dataAPattern = getLGDPropertyValue("params.dataa.pattern");

            logger.debug("csvFormat: " + csvFormat);
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

    public String getLGDPropertyValue(String key) {
        return propertiesConfiguration.getString(key);
    }

    private void getSparkSessionWithUDFs(){

        sparkSession = new SparkSession.Builder().getOrCreate();

        registerUDFsForDateComparison();
        registerUDFsForDateManipulation();

    }

    private void registerUDFsForDateComparison(){

        // UDFS FOR DATE COMPARISON (EACH RETURNS A BOOLEAN)
        sparkSession.udf().register("isDateGtOtherDate", UDFsFactory.isDateGtOtherDateUDF(), DataTypes.BooleanType);
        sparkSession.udf().register("isDateGeqOtherDate", UDFsFactory.isDateGeqOtherDateUDF(), DataTypes.BooleanType);
        sparkSession.udf().register("isDateLtOtherDate", UDFsFactory.isDateLtOtherDateUDF(), DataTypes.BooleanType);
        sparkSession.udf().register("isDateLeqOtherDate", UDFsFactory.isDateLeqOtherDateUDF(), DataTypes.BooleanType);
        sparkSession.udf().register("dateBetween", UDFsFactory.isDateBetweenLowerDateAndUpperDateUDF(), DataTypes.BooleanType);
    }

    private void registerUDFsForDateManipulation(){

        // UDFS FOR DATE MANIPULATION
        sparkSession.udf().register("addDuration", UDFsFactory.addDurationUDF(), DataTypes.StringType);
        sparkSession.udf().register("substractDuration", UDFsFactory.substractDurationUDF(), DataTypes.StringType);
        sparkSession.udf().register("changeDateFormat", UDFsFactory.changeDateFormatUDF(), DataTypes.StringType);
        sparkSession.udf().register("greatestDate", UDFsFactory.greatestDateUDF(), DataTypes.StringType);
        sparkSession.udf().register("leastDate", UDFsFactory.leastDateUDF(), DataTypes.StringType);
        sparkSession.udf().register("daysBetween", UDFsFactory.daysBetweenUDF(), DataTypes.LongType);

    }

    abstract public void run();
}
