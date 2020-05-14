package it.carloni.luca.lgd.spark.udf;

import org.apache.spark.sql.api.java.UDF3;

import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class UDFFactory {

    // adds numberOfMonths to a date with pattern datePattern
    // equivalent of PIG function AddDuration(ToDate((chararray)datainiziodef,'yyyyMMdd'),'$numero_mesi_1')
    public static UDF3<String, String, Integer, String> buildAddDurationUDF(){

        return (UDF3<String, String, Integer, String>) (date, datePattern, numberOfMonths) -> {

            try {

                DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(datePattern);
                LocalDate localDate = LocalDate.parse(date, dateTimeFormatter);
                return localDate.plusMonths(numberOfMonths).format(dateTimeFormatter);
            }

            catch (NullPointerException | DateTimeParseException e) { return null; }};
    }

    // changed format of date inputDate from oldPattern to newPattern
    public static UDF3<String, String, String, String> buildChangeDateFormatUDF(){

        return (UDF3<String, String, String, String>) (inputDate, oldPattern, newPattern) -> {

           try {

               return LocalDate.parse(inputDate, DateTimeFormatter.ofPattern(oldPattern))
                       .format(DateTimeFormatter.ofPattern(newPattern));
           }

           catch (NullPointerException | DateTimeException e) { return null; }};
    }

    // return the number of days between two dates with same pattern, in absolute value
    // DaysBetween( ToDate((chararray)tlbcidef::datafinedef,'yyyyMMdd' ), ToDate((chararray)tlbpaspe_filter::datacont,'yyyyMMdd' ) ) as days_diff
    public static UDF3<String, String, String, Long> buildDaysBetweenUDF(){

        return (UDF3<String, String, String, Long>) (stringFirstDate, stringSecondDate, commonPattern) -> {

            try {

                DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(commonPattern);
                LocalDate firstDate = LocalDate.parse(stringFirstDate, dateTimeFormatter);
                LocalDate secondDate = LocalDate.parse(stringSecondDate, dateTimeFormatter);
                return Math.abs(firstDate.minusDays(secondDate.toEpochDay()).toEpochDay()); }

            catch (NullPointerException | DateTimeException e) { return null;} };
    }

    // return the greatest date between two dates with same pattern
    public static UDF3<String, String, String, String> buildGreatestDateUDF(){

        return (UDF3<String, String, String, String>)
                (stringFirstDate, stringSecondDate, commonPattern) -> {

                    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(commonPattern);
                    LocalDate firstDate = LocalDate.parse(stringFirstDate, dateTimeFormatter);
                    LocalDate secondDate = LocalDate.parse(stringSecondDate, dateTimeFormatter);
                    return firstDate.compareTo(secondDate) >= 0 ? firstDate.format(dateTimeFormatter) : secondDate.format(dateTimeFormatter);
                };
    }

    // return the least date between two dates with same pattern
    public static UDF3<String, String, String, String> buildLeastDateUDF(){

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
    public static UDF3<String, String, Integer, String> buildSubstractDurationUDF(){

        return  (UDF3<String, String, Integer, String>) (date, datePattern, numberOfMonths) -> {

            try {

                DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(datePattern);
                LocalDate localDate = LocalDate.parse(date, dateTimeFormatter);
                return localDate.minusMonths(numberOfMonths).format(dateTimeFormatter);
            }

            catch (NullPointerException | DateTimeException e) { return null;}};
    }
}
