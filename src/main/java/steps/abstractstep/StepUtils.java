package steps.abstractstep;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.*;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

abstract class StepUtils {

    protected Column addDuration(Column dateCol, String dateColFormat, int numberOfMonths){

        return functions.callUDF("addDuration", dateCol, functions.lit(dateColFormat), functions.lit(numberOfMonths));
    }

    // change the format of string expressing a date
    protected Column changeDateFormat(Column dateColumn, String oldPattern, String newPattern){

        return functions.callUDF("changeDateFormat", dateColumn, functions.lit(oldPattern), functions.lit(newPattern));
    }

    // change date format from oldPattern to newPattern
    protected String changeDateFormat(String date, String oldPattern, String newPattern){

        return LocalDate.parse(date, DateTimeFormatter.ofPattern(oldPattern)).format(DateTimeFormatter.ofPattern(newPattern));
    }

    // check if a date is within a given interval
    protected Column dateBetween(Column dateColumn, String dateColumnPattern, String lowerDate, String lowerDatePattern,
                                 String upperDate, String upperDatePattern){

        return functions.callUDF("dateBetween",
                dateColumn, functions.lit(dateColumnPattern),
                functions.lit(lowerDate), functions.lit(lowerDatePattern),
                functions.lit(upperDate), functions.lit(upperDatePattern));
    }

    // check if a date is within a given interval
    protected Column dateBetween(Column dateColumn, String dateColumnPattern, Column lowerDate, String lowerDatePattern,
                                 Column upperDate, String upperDatePattern){

        return functions.callUDF("dateBetween",
                dateColumn, functions.lit(dateColumnPattern),
                lowerDate, functions.lit(lowerDatePattern),
                upperDate, functions.lit(upperDatePattern));
    }

    // check if a date is > other date
    protected Column dateGtOtherDate(Column dateColumn, String dateColumnPattern, String otherDate, String otherDatePattern){

        return functions.callUDF("date1GtDate2",
                dateColumn, functions.lit(dateColumnPattern),
                functions.lit(otherDate), functions.lit(otherDatePattern));
    }

    // check if a date is <= other date
    protected Column dateLeqOtherDate(Column dateColumn, String dateColumnPattern, String otherDate, String otherDatePattern){

        return functions.callUDF("date1LeqDate2",
                dateColumn, functions.lit(dateColumnPattern),
                functions.lit(otherDate), functions.lit(otherDatePattern));
    }

    // check if a date is < other date
    protected Column dateLtOtherDate(Column dateColumn, String dateColumnPattern, String otherDate, String otherDatePattern){

        return functions.callUDF("date1LtDate2",
                dateColumn, functions.lit(dateColumnPattern),
                functions.lit(otherDate), functions.lit(otherDatePattern));
    }

    protected Column dateLtOtherDate(Column dateColumn, String dateColumnPattern, Column otherDateColumn, String otherDatePattern){

        return functions.callUDF("date1LtDate2",
                dateColumn, functions.lit(dateColumnPattern),
                otherDateColumn, functions.lit(otherDatePattern));
    }

    protected Column daysBetween(Column dateCol1, Column dateCol2, String commonPattern){

        return functions.callUDF("daysBetween", dateCol1, dateCol2, functions.lit(commonPattern));
    }

    protected Column getQuadJoinCondition(Dataset<Row> datasetLeft, Dataset<Row> datasetRight, List<String> joinColumnNames){

        Column joinCondition = datasetLeft.col(joinColumnNames.get(0))
                .equalTo(datasetRight.col(joinColumnNames.get(0).toUpperCase()));

        for (String joinColumnName: joinColumnNames.subList(1, joinColumnNames.toArray().length - 1)){

            joinCondition = joinCondition.and(datasetLeft.col(joinColumnName)
                    .equalTo(datasetRight.col(joinColumnName.toUpperCase())));
        }

        return joinCondition;
    }

    // create a schema with one String column for each name provided
    protected StructType getStringTypeSchema(List<String> columnNames){

        StructType schema = new StructType();
        for (String columnName: columnNames){

            schema = schema.add(new StructField(columnName, DataTypes.StringType, true, Metadata.empty()));
        }

        return schema;
    }

    protected Column leastDate(Column dateColumn1, Column dateColumn2, String commonDateFormat){

        return functions.callUDF("leastDate", dateColumn1, dateColumn2, functions.lit(commonDateFormat));
    }

    // convert a string into a LocalDate object
    protected LocalDate parseStringToLocalDate(String stringDate, String pattern){

        return LocalDate.parse(stringDate, DateTimeFormatter.ofPattern(pattern));
    }

    // replace oldString with newString and convert to Double column
    protected Column replaceAndConvertToDouble(Dataset<Row> df, String columnName, String oldString, String newString){

        return functions.regexp_replace(df.col(columnName), oldString, newString)
                .cast(DataTypes.DoubleType).as(columnName);
    }

    // create a list of columns to be selected from the given dataset
    protected List<Column> selectDfColumns(Dataset<Row> df, List<String> columnNames){

        List<Column> dfCols = new ArrayList<>();
        for (String columnName: columnNames){
            dfCols.add(df.col(columnName));
        }

        return dfCols;
    }

    // create a list of columns to be selected from the given dataset, giving an alias to each column
    protected List<Column> selectDfColumns(Dataset<Row> df, Map<String,String> columnMap){

        List<Column> dfCols = new ArrayList<>();
        Set<Map.Entry<String, String>> entryList = columnMap.entrySet();
        for (Map.Entry<String, String> entry: entryList){

            dfCols.add(df.col(entry.getKey()).alias(entry.getValue()));
        }

        return dfCols;
    }

    protected Column substringAndCastToInt(Column column, int startIndex, int length){

        return functions.substring(column, startIndex, length).cast(DataTypes.IntegerType);
    }

    protected Column subtractDuration(Column dateCol, String dateColFormat, int months){

        return functions.callUDF("substractDuration", dateCol, functions.lit(dateColFormat), functions.lit(months));
    }

    protected Seq<String> toScalaStringSeq(List<String> list){
        return JavaConversions.asScalaBuffer(list).toSeq();
    }

    protected Seq<Column> toScalaColSeq(List<Column> list) { return JavaConversions.asScalaBuffer(list).toSeq();}

    // creates a list of aggregate column expressions to be used over windowspec w on dataframe df
    protected List<Column> windowSum(Dataset<Row> df, Map<String, String> columnMap, WindowSpec w){

        List<Column> columnList = new ArrayList<>();
        Set<Map.Entry<String, String>> entryList = columnMap.entrySet();
        for (Map.Entry<String, String> entry: entryList){

            columnList.add(functions.sum(df.col(entry.getKey())).over(w).alias(entry.getValue()));
        }
        return columnList;
    }
}

