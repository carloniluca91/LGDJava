package it.carloni.luca.lgd.spark.utils;

import it.carloni.luca.lgd.spark.udf.UDFName;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class StepUtils {

    /***
     * UDF that adds numberOfMonths to a String expressing a date with format dateColFormat
     * @param dateCol: String column expressing a date
     * @param dateColFormat: format of String column
     * @param numberOfMonths: number of months to add
     * @return: dateCol.plusMonths(numberOfMonths)
     */

    public static Column addDurationUDF(Column dateCol, String dateColFormat, int numberOfMonths){

        return functions.callUDF(UDFName.ADD_DURATION.getName(),
                dateCol,
                functions.lit(dateColFormat),
                functions.lit(numberOfMonths));
    }

    /***
     * UDF that formats a String expressing a date from oldPattern to newPattern
     * @param dateColumn: String column expressing a date
     * @param oldPattern: old format of String column
     * @param newPattern: new format of String column
     * @return: dateColumn with format updated to newPattern
     */

    public static Column changeDateFormatUDF(Column dateColumn, String oldPattern, String newPattern) {

        return functions.callUDF(UDFName.CHANGE_DATE_FORMAT.getName(),
                dateColumn,
                functions.lit(oldPattern),
                functions.lit(newPattern));
    }

    /***
     * UDF that formats a String expressing a date from oldPattern to newPattern
     * To be used for converting a date from a format with yy to another with yyyy
     * @param dateColumn: String column expressing a date
     * @param oldPattern: old format of String column
     * @param newPattern: new format of String column
     * @return: dateColumn with format updated to newPattern
     */

    public static Column changeDateFormatFromY2toY4UDF(Column dateColumn, String oldPattern, String newPattern) {

        return functions.callUDF(UDFName.CHANGE_DATE_FORMAT_FROM_Y2_TO_Y4.getName(),
                dateColumn,
                functions.lit(oldPattern),
                functions.lit(newPattern));
    }

    public static String changeDateFormat(String date, String oldPattern, String newPattern){

        return LocalDate.parse(date, DateTimeFormatter.ofPattern(oldPattern))
                .format(DateTimeFormatter.ofPattern(newPattern));
    }

    /***
     * UDF that computes the number of days between two dates (both with pattern "commonPattern")
     * @param dateCol1: first date (Column)
     * @param dateCol2: second date (Column)
     * @param commonPattern: first and second date pattern
     * @return: Integer column
     */

    public static Column daysBetweenUDF(Column dateCol1, Column dateCol2, String commonPattern){

        return functions.callUDF(UDFName.DAYS_BETWEEN.getName(),
                dateCol1,
                dateCol2,
                functions.lit(commonPattern));
    }

    /***
     * UDF that computes the least date between two dates expressed by two strings with same format (commonDateFormat)
     * @param dateColumn1: first date (String Column)
     * @param dateColumn2: second date (String Column)
     * @param commonDateFormat: common date format
     * @return: String Column
     */

    public static Column leastDateUDF(Column dateColumn1, Column dateColumn2, String commonDateFormat){

        return functions.callUDF(UDFName.LEAST_DATE.getName(),
                dateColumn1,
                dateColumn2,
                functions.lit(commonDateFormat));
    }

    public static LocalDate parseStringToLocalDate(String stringDate, String pattern){

        return LocalDate.parse(stringDate, DateTimeFormatter.ofPattern(pattern));
    }

    public static Column substringAndToInt(Column column, int startIndex, int length){

        return functions.substring(column.cast(DataTypes.StringType), startIndex, length).cast(DataTypes.IntegerType);
    }

    /***
     * UDF that subtracts numberOfMonths to a date expressed as String with format dateColFormat
     * @param dateCol: date (String Column)
     * @param dateColFormat: date format
     * @param numberOfMonths: number of months to subtract
     * @return: String Column
     */

    public static Column subtractDurationUDF(Column dateCol, String dateColFormat, int numberOfMonths) {

        return functions.callUDF(UDFName.SUBTRACT_DURATION.getName(),
                dateCol,
                functions.lit(dateColFormat),
                functions.lit(numberOfMonths));
    }

    public static <T> Seq<T> toScalaSeq(List<T> javaList) {

        return JavaConversions.asScalaBuffer(javaList).toSeq();
    }

    public static Column toIntCol(Column column){

        return column.cast(DataTypes.IntegerType);
    }

    public static Column toStringCol(Column column){

        return column.cast(DataTypes.StringType);
    }
}

