package steps.abstractstep;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import steps.abstractstep.udfs.UDFsFactory;
import steps.abstractstep.udfs.UDFsNameEnum;

public abstract class AbstractStep {

    protected final Logger logger = Logger.getLogger(AbstractStep.class);
    private final PropertiesConfiguration propertiesConfiguration = new PropertiesConfiguration();
    protected SparkSession sparkSession;

    // input and output dirs for a step
    protected String stepInputDir;
    protected String stepOutputDir;

    protected String dataDaPattern;
    protected String dataAPattern;
    protected String csvFormat;

    protected AbstractStep(){

        try {

            propertiesConfiguration.load(AbstractStep.class.getClassLoader().getResourceAsStream("lgd.properties"));

            csvFormat = getValue("csv.format");
            dataDaPattern = getValue("params.datada.pattern");
            dataAPattern = getValue("params.dataa.pattern");

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

    public String getValue(String key) {
        return propertiesConfiguration.getString(key);
    }

    private void getSparkSessionWithUDFs(){

        sparkSession = SparkSession.builder().getOrCreate();
        registerUDFsForDateComparison();
        registerUDFsForDateManipulation();
    }

    private void registerUDFsForDateComparison(){

        // UDFS FOR DATE COMPARISON (EACH RETURNS A BOOLEAN)
        sparkSession.udf().register(UDFsNameEnum.IS_DATE_GT_OTHERDATE_UDF_NAME, UDFsFactory.isDateGtOtherDateUDF(), DataTypes.BooleanType);
        sparkSession.udf().register(UDFsNameEnum.IS_DATE_GEQ_OTHERDATE_UDF_NAME, UDFsFactory.isDateGeqOtherDateUDF(), DataTypes.BooleanType);
        sparkSession.udf().register(UDFsNameEnum.IS_DATE_LT_OTHERDATE_UDF_NAME, UDFsFactory.isDateLtOtherDateUDF(), DataTypes.BooleanType);
        sparkSession.udf().register(UDFsNameEnum.IS_DATE_LEQ_OTHERDATE_UDF_NAME, UDFsFactory.isDateLeqOtherDateUDF(), DataTypes.BooleanType);
        sparkSession.udf().register(UDFsNameEnum.IS_DATE_BETWEEN_UDF_NAME, UDFsFactory.isDateBetweenLowerDateAndUpperDateUDF(), DataTypes.BooleanType);
    }

    private void registerUDFsForDateManipulation(){

        // UDFS FOR DATE MANIPULATION
        sparkSession.udf().register(UDFsNameEnum.ADD_DURATION_UDF_NAME, UDFsFactory.addDurationUDF(), DataTypes.StringType);
        sparkSession.udf().register(UDFsNameEnum.SUBTRACT_DURATION_UDF_NAME, UDFsFactory.substractDurationUDF(), DataTypes.StringType);
        sparkSession.udf().register(UDFsNameEnum.CHANGE_DATE_FORMAT_UDF_NAME, UDFsFactory.changeDateFormatUDF(), DataTypes.StringType);
        sparkSession.udf().register(UDFsNameEnum.GREATEST_DATE_UDF_NAME, UDFsFactory.greatestDateUDF(), DataTypes.StringType);
        sparkSession.udf().register(UDFsNameEnum.LEAST_DATE_UDF_NAME, UDFsFactory.leastDateUDF(), DataTypes.StringType);
        sparkSession.udf().register(UDFsNameEnum.DAYS_BETWEEN_UDF_NAME, UDFsFactory.daysBetweenUDF(), DataTypes.LongType);
    }

    abstract public void run();
}
