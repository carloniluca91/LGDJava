package it.carloni.luca.lgd.spark.udf;

import org.apache.spark.sql.api.java.UDF3;

import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class UDFFactory {

    // ADDS NUMBEROFMONTHS TO A DATE WITH PATTERN DATEPATTERN
    // EQUIVALENT OF PIG FUNCTION ADDDURATION(TODATE((CHARARRAY)DATAINIZIODEF,'YYYYMMDD'),'$NUMERO_MESI_1')
    public static UDF3<String, String, Integer, String> buildAddDurationUDF(){

        return (UDF3<String, String, Integer, String>) (date, datePattern, numberOfMonths) -> {

            try {

                DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(datePattern);
                LocalDate localDate = LocalDate.parse(date, dateTimeFormatter);
                return localDate.plusMonths(numberOfMonths).format(dateTimeFormatter);
            }

            catch (NullPointerException | DateTimeParseException e) { return null; }};
    }

    // CHANGES FORMAT OF DATE (EXPRESSED BY A STRING) FROM OLDPATTERN TO NEWPATTERN
    public static UDF3<String, String, String, String> buildChangeDateFormatUDF(){

        return (UDF3<String, String, String, String>) (inputDate, oldPattern, newPattern) -> {

           try {

               return LocalDate.parse(inputDate, DateTimeFormatter.ofPattern(oldPattern))
                       .format(DateTimeFormatter.ofPattern(newPattern));
           }

           catch (NullPointerException | DateTimeException e) { return null; }};
    }

    // CHANGES FORMAT OF DATE (EXPRESSED BY A STRING) FROM OLDPATTERN TO NEWPATTERN
    // TO BE USED WHEN PASSING FROM yy TO yyyy YEAR FORMAT
    public static UDF3<String, String, String, String> buildChangeDateFormatFromY2toY4() {

        return (UDF3<String, String, String, String>) (inputDate, oldPattern, newPattern) -> {

            try {

                DateTimeFormatter newPatternFormatter = DateTimeFormatter.ofPattern(newPattern);
                LocalDate inputDateAsLocalDate = LocalDate.parse(inputDate, DateTimeFormatter.ofPattern(oldPattern));
                return inputDateAsLocalDate.isAfter(LocalDate.now()) ?
                        inputDateAsLocalDate.minusYears(100).format(newPatternFormatter):
                        inputDateAsLocalDate.format(newPatternFormatter);
            }

            catch (NullPointerException | DateTimeException e) {return null; }};
    }

    // RETURNS THE NUMBER OF DAYS BETWEEN TWO DATES WITH SAME PATTERN, IN ABSOLUTE VALUE
    // DAYSBETWEEN( TODATE((CHARARRAY)TLBCIDEF::DATAFINEDEF,'YYYYMMDD' ), TODATE((CHARARRAY)TLBPASPE_FILTER::DATACONT,'YYYYMMDD' ) ) AS DAYS_DIFF
    public static UDF3<String, String, String, Long> buildDaysBetweenUDF(){

        return (UDF3<String, String, String, Long>) (stringFirstDate, stringSecondDate, commonPattern) -> {

            try {

                DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(commonPattern);
                LocalDate firstDate = LocalDate.parse(stringFirstDate, dateTimeFormatter);
                LocalDate secondDate = LocalDate.parse(stringSecondDate, dateTimeFormatter);
                return Math.abs(firstDate.minusDays(secondDate.toEpochDay()).toEpochDay()); }

            catch (NullPointerException | DateTimeException e) { return null;} };
    }

    // RETURN THE GREATEST DATE BETWEEN TWO DATES WITH SAME PATTERN
    public static UDF3<String, String, String, String> buildGreatestDateUDF(){

        return (UDF3<String, String, String, String>)
                (stringFirstDate, stringSecondDate, commonPattern) -> {

                    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(commonPattern);
                    LocalDate firstDate = LocalDate.parse(stringFirstDate, dateTimeFormatter);
                    LocalDate secondDate = LocalDate.parse(stringSecondDate, dateTimeFormatter);
                    return firstDate.compareTo(secondDate) >= 0 ? firstDate.format(dateTimeFormatter) : secondDate.format(dateTimeFormatter);
                };
    }

    // RETURN THE LEAST DATE BETWEEN TWO DATES WITH SAME PATTERN
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

    // ADDS NUMBEROFMONTHS TO A DATE WITH PATTERN DATEPATTERN
    // EQUIVALENT OF PIG FUNCTION SUBTRACTDURATION(TODATE((CHARARRAY)DATAINIZIODEF,'YYYYMMDD'),'$NUMERO_MESI_1')
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
