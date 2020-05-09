package it.carloni.luca.lgd.spark.udf;

import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.api.java.UDF6;

import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class UDFFactory {

    // adds numberOfMonths to a date with pattern datePattern
    // equivalent of PIG function AddDuration(ToDate((chararray)datainiziodef,'yyyyMMdd'),'$numero_mesi_1')
    public static UDF3<String, String, Integer, String> addDurationUDF(){

        return (UDF3<String, String, Integer, String>) (date, datePattern, numberOfMonths) -> {

            try {

                DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(datePattern);
                LocalDate localDate = LocalDate.parse(date, dateTimeFormatter);
                return localDate.plusMonths(numberOfMonths).format(dateTimeFormatter);
            }

            catch (NullPointerException | DateTimeParseException e) { return null; }};
    }

    // changed format of date inputDate from oldPattern to newPattern
    public static UDF3<String, String, String, String> changeDateFormatUDF(){

        return (UDF3<String, String, String, String>) (inputDate, oldPattern, newPattern) -> {

           try {

               return LocalDate.parse(inputDate, DateTimeFormatter.ofPattern(oldPattern))
                       .format(DateTimeFormatter.ofPattern(newPattern));
           }

           catch (NullPointerException | DateTimeException e) { return null; }};
    }

    // return the number of days between two dates with same pattern, in absolute value
    // DaysBetween( ToDate((chararray)tlbcidef::datafinedef,'yyyyMMdd' ), ToDate((chararray)tlbpaspe_filter::datacont,'yyyyMMdd' ) ) as days_diff
    public static UDF3<String, String, String, Long> daysBetweenUDF(){

        return (UDF3<String, String, String, Long>) (stringFirstDate, stringSecondDate, commonPattern) -> {

            try {

                DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(commonPattern);
                LocalDate firstDate = LocalDate.parse(stringFirstDate, dateTimeFormatter);
                LocalDate secondDate = LocalDate.parse(stringSecondDate, dateTimeFormatter);
                return Math.abs(firstDate.minusDays(secondDate.toEpochDay()).toEpochDay()); }

            catch (NullPointerException | DateTimeException e) { return null;} };
    }

    // return the greatest date between two dates with same pattern
    public static UDF3<String, String, String, String> greatestDateUDF(){

        return (UDF3<String, String, String, String>)
                (stringFirstDate, stringSecondDate, commonPattern) -> {

                    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(commonPattern);
                    LocalDate firstDate = LocalDate.parse(stringFirstDate, dateTimeFormatter);
                    LocalDate secondDate = LocalDate.parse(stringSecondDate, dateTimeFormatter);
                    return firstDate.compareTo(secondDate) >= 0 ? firstDate.format(dateTimeFormatter) : secondDate.format(dateTimeFormatter);
                };
    }

    // returns true if stringDate (with pattern datePattern) is between
    // stringLowerDate (with pattern lowerDatePattern) and stringUpperDate (with pattern upperDatePattern)
    public static UDF6<String, String, String, String, String, String, Boolean> isDateBetweenLowerDateAndUpperDateUDF(){

        return (UDF6<String, String, String, String, String, String, Boolean>)
                (stringDate, datePattern, stringLowerDate, lowerDatePattern, stringUpperDate, upperDatePattern)-> {

            try {

                LocalDate localDate = LocalDate.parse(stringDate, DateTimeFormatter.ofPattern(datePattern));
                LocalDate lowerDate = LocalDate.parse(stringLowerDate, DateTimeFormatter.ofPattern(lowerDatePattern));
                LocalDate upperDate = LocalDate.parse(stringUpperDate, DateTimeFormatter.ofPattern(upperDatePattern));
                return (localDate.compareTo(lowerDate) >= 0) & (localDate.compareTo(upperDate) <= 0);
            }

            catch (NullPointerException | DateTimeException e) { return null;}};
    }

    // return the least date between two dates with same pattern
    public static UDF3<String, String, String, String> leastDateUDF(){

        return (UDF3<String, String, String, String>) (stringFirstDate, stringSecondDate, commonPattern) -> {

            try {

                DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(commonPattern);
                LocalDate firstDate = LocalDate.parse(stringFirstDate, dateTimeFormatter);
                LocalDate secondDate = LocalDate.parse(stringSecondDate, dateTimeFormatter);
                return firstDate.compareTo(secondDate) <= 0 ? firstDate.format(dateTimeFormatter) : secondDate.format(dateTimeFormatter);
            }

            catch (NullPointerException | DateTimeException e) { return null;}};
    }

    // adds numberOfMonths to a date with pattern datePattern
    // equivalent of PIG function SubtractDuration(ToDate((chararray)datainiziodef,'yyyyMMdd'),'$numero_mesi_1')
    public static UDF3<String, String, Integer, String> substractDurationUDF(){

        return  (UDF3<String, String, Integer, String>) (date, datePattern, numberOfMonths) -> {

            try {

                DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(datePattern);
                LocalDate localDate = LocalDate.parse(date, dateTimeFormatter);
                return localDate.minusMonths(numberOfMonths).format(dateTimeFormatter);
            }

            catch (NullPointerException | DateTimeException e) { return null;}};
    }
}
