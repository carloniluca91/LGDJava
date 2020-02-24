package it.carloni.luca.lgd.commons;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.*;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import it.carloni.luca.lgd.commons.udfs.UDFsNames;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class StepUtils {

    /***
     * calls a registered UDF in order to add numberOfMonths to a String expressing a date
     * @param dateCol: String column expressing a date
     * @param dateColFormat: format of String column
     * @param numberOfMonths: number of months to add
     * @return: dateCol.plusMonths(numberOfMonths)
     */

    public static Column addDuration(Column dateCol, String dateColFormat, int numberOfMonths){

        return functions.callUDF(UDFsNames.ADD_DURATION_UDF_NAME,
                dateCol, functions.lit(dateColFormat), functions.lit(numberOfMonths));
    }

    /***
     * calls a registered UDF in order to changethe format of a String expressing a date
     * @param dateColumn: String column expressing a date
     * @param oldPattern: old format of String column
     * @param newPattern: new format of String column
     * @return: dateColumn with format updated to newPattern
     */

    public static Column changeDateFormat(Column dateColumn, String oldPattern, String newPattern){

        return functions.callUDF(UDFsNames.CHANGE_DATE_FORMAT_UDF_NAME,
                dateColumn, functions.lit(oldPattern),
                functions.lit(newPattern));
    }

    /***
     * format a String expressing a date from one format to antoher
     * @param date: String object expressing a date
     * @param oldPattern: old format of date
     * @param newPattern: new format of date
     * @return: date with format updated to newPattern
     */

    public static String changeDateFormat(String date, String oldPattern, String newPattern){

        return LocalDate.parse(date, DateTimeFormatter.ofPattern(oldPattern))
                .format(DateTimeFormatter.ofPattern(newPattern));
    }

    /***
     * calls a registered UDF in order to check if a date with pattern dateColumnPattern is within defined by
     * lowerDate (with pattern lowerDatePattern) and upperDate (with pattern upperDatePattern)
     * @param dateColumn: String column expressing a date
     * @param dateColumnPattern: format of String column
     * @param lowerDate: lower date (String)
     * @param lowerDatePattern: lower date pattern
     * @param upperDate: upper date (String)
     * @param upperDatePattern: upper date pattern
     * @return: boolean column
     */

    public static Column isDateBetween(Column dateColumn, String dateColumnPattern,
                                       String lowerDate, String lowerDatePattern,
                                       String upperDate, String upperDatePattern){

        return functions.callUDF(UDFsNames.IS_DATE_BETWEEN_UDF_NAME,
                dateColumn, functions.lit(dateColumnPattern),
                functions.lit(lowerDate), functions.lit(lowerDatePattern),
                functions.lit(upperDate), functions.lit(upperDatePattern));
    }

    /***
     * calls a registered UDF in order to check if a date with pattern dateColumnPattern is within defined by
     * lowerDate (with pattern lowerDatePattern) and upperDate (with pattern upperDatePattern)
     * @param dateColumn: String column expressing a date
     * @param dateColumnPattern: format of String column
     * @param lowerDate: lower date (Column)
     * @param lowerDatePattern: lower date pattern
     * @param upperDate: upper date (Column)
     * @param upperDatePattern: upper date pattern
     * @return: boolean column
     */

    // check if a date is within a given interval
    public static Column isDateBetween(Column dateColumn, String dateColumnPattern,
                                       Column lowerDate, String lowerDatePattern,
                                       Column upperDate, String upperDatePattern){

        return functions.callUDF(UDFsNames.IS_DATE_BETWEEN_UDF_NAME,
                dateColumn, functions.lit(dateColumnPattern),
                lowerDate, functions.lit(lowerDatePattern),
                upperDate, functions.lit(upperDatePattern));
    }

    public static Column daysBetween(Column dateCol1, Column dateCol2, String commonPattern){

        return functions.callUDF(UDFsNames.DAYS_BETWEEN_UDF_NAME,
                dateCol1, dateCol2, functions.lit(commonPattern));
    }

    public static Column getQuadJoinCondition(Dataset<Row> datasetLeft, Dataset<Row> datasetRight, List<String> joinColumnNames){

        Column joinCondition = datasetLeft.col(joinColumnNames.get(0))
                .equalTo(datasetRight.col(joinColumnNames.get(0).toUpperCase()));

        for (String joinColumnName: joinColumnNames.subList(1, joinColumnNames.toArray().length - 1)){

            joinCondition = joinCondition.and(datasetLeft.col(joinColumnName)
                    .equalTo(datasetRight.col(joinColumnName.toUpperCase())));
        }

        return joinCondition;
    }

    // create a schema with one String column for each name provided
    public static StructType fromPigSchemaToStructType(Map<String, String> pigSchema){

        StructType schema = new StructType();
        for (Map.Entry<String, String> pigSchemaEntry : pigSchema.entrySet()){
            
            String columnName = pigSchemaEntry.getKey();
            DataType dataType = resolveDataType(pigSchemaEntry.getValue());
            schema = schema.add(new StructField(columnName, dataType, true, Metadata.empty()));
        }

        return schema;
    }

    public static Column leastDate(Column dateColumn1, Column dateColumn2, String commonDateFormat){

        return functions.callUDF(UDFsNames.LEAST_DATE_UDF_NAME,
                dateColumn1,
                dateColumn2,
                functions.lit(commonDateFormat));
    }

    // convert a string into a LocalDate object
    public static LocalDate parseStringToLocalDate(String stringDate, String pattern){

        return LocalDate.parse(stringDate, DateTimeFormatter.ofPattern(pattern));
    }

    // replace oldString with newString and convert to Double column
    public static Column replaceAndConvertToDouble(Dataset<Row> df, String columnName, String oldString, String newString){

        return functions.regexp_replace(df.col(columnName), oldString, newString)
                .cast(DataTypes.DoubleType).as(columnName);
    }
    
    private static DataType resolveDataType(String pigColumnType){
        
        DataType dataType;
        switch (pigColumnType){

            case "int": dataType = DataTypes.IntegerType; break;
            case "double": dataType = DataTypes.DoubleType; break;
            default: dataType = DataTypes.StringType; break;
        }
        
        return dataType;
    }

    // create a list of columns to be selected from the given dataset
    public static List<Column> selectDfColumns(Dataset<Row> df, List<String> columnNames){

        List<Column> dfCols = new ArrayList<>();
        for (String columnName: columnNames){
            dfCols.add(df.col(columnName));
        }

        return dfCols;
    }

    // create a list of columns to be selected from the given dataset, giving an alias to each column
    public static List<Column> selectDfColumns(Dataset<Row> df, Map<String,String> columnMap){

        List<Column> dfCols = new ArrayList<>();
        Set<Map.Entry<String, String>> entryList = columnMap.entrySet();
        for (Map.Entry<String, String> entry: entryList){

            dfCols.add(df.col(entry.getKey()).alias(entry.getValue()));
        }

        return dfCols;
    }

    public static Column substringAndCastToInt(Column column, int startIndex, int length){

        return functions.substring(column, startIndex, length).cast(DataTypes.IntegerType);
    }

    public static Column subtractDuration(Column dateCol, String dateColFormat, int months){

        return functions.callUDF(UDFsNames.SUBTRACT_DURATION_UDF_NAME,
                dateCol,
                functions.lit(dateColFormat),
                functions.lit(months));
    }

    public static Seq<String> toScalaStringSeq(List<String> list){
        return JavaConversions.asScalaBuffer(list).toSeq();
    }

    public static Seq<Column> toScalaColSeq(List<Column> list) { return JavaConversions.asScalaBuffer(list).toSeq();}

    public static Column toIntCol(Column column){

        return column.cast(DataTypes.IntegerType);
    }

    public static Column toStringCol(Column column){

        return column.cast(DataTypes.StringType);
    }

    // creates a list of aggregate column expressions to be used over windowspec w on dataframe df
    public static List<Column> windowSum(Dataset<Row> df, Map<String, String> columnMap, WindowSpec w){

        List<Column> columnList = new ArrayList<>();
        Set<Map.Entry<String, String>> entryList = columnMap.entrySet();
        for (Map.Entry<String, String> entry: entryList){

            columnList.add(functions.sum(df.col(entry.getKey())).over(w).alias(entry.getValue()));
        }
        return columnList;
    }
}

