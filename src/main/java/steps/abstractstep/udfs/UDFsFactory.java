package steps.abstractstep.udfs;

import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.api.java.UDF6;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class UDFsFactory {

    // adds numberOfMonths to a date with pattern datePattern
    // equivalent of PIG function AddDuration(ToDate((chararray)datainiziodef,'yyyyMMdd'),'$numero_mesi_1')
    public static UDF3<String, String, Integer, String> addDurationUDF(){

        return (UDF3<String, String, Integer, String>)
                (date, datePattern, numberOfMonths) -> {

                    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(datePattern);
                    LocalDate localDate = LocalDate.parse(date, dateTimeFormatter);
                    return localDate.plusMonths(numberOfMonths).format(dateTimeFormatter);

                };
    }

    // changed format of date inputDate from oldPattern to newPattern
    public static UDF3<String, String, String, String> changeDateFormatUDF(){

        return (UDF3<String, String, String, String>)
                (inputDate, oldPattern, newPattern) -> {

                    if (inputDate != null){
                        LocalDate localDate = LocalDate.parse(inputDate, DateTimeFormatter.ofPattern(oldPattern));
                        return localDate.format(DateTimeFormatter.ofPattern(newPattern));
                    }
                    else { return null; }
                };
    }

    // return the number of days between two dates with same pattern, in absolute value
    // DaysBetween( ToDate((chararray)tlbcidef::datafinedef,'yyyyMMdd' ), ToDate((chararray)tlbpaspe_filter::datacont,'yyyyMMdd' ) ) as days_diff
    public static UDF3<String, String, String, Long> daysBetweenUDF(){

        return (UDF3<String, String, String, Long>)
                (stringFirstDate, stringSecondDate, commonPattern) -> {

                    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(commonPattern);
                    LocalDate firstDate = LocalDate.parse(stringFirstDate, dateTimeFormatter);
                    LocalDate secondDate = LocalDate.parse(stringSecondDate, dateTimeFormatter);
                    return Math.abs(firstDate.minusDays(secondDate.toEpochDay()).toEpochDay());

                };
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

    // returns true if date1 (with pattern date1Pattern) > date 2 (with pattern date2Pattern)
    public static UDF4<String, String, String, String, Boolean> isDateGtOtherDateUDF(){

        return (UDF4<String, String, String, String, Boolean>)
                (date1, date1Pattern, date2, date2Pattern) -> {

                    LocalDate localDate1 = LocalDate.parse(date1, DateTimeFormatter.ofPattern(date1Pattern));
                    LocalDate localDate2 = LocalDate.parse(date2, DateTimeFormatter.ofPattern(date2Pattern));
                    return localDate1.compareTo(localDate2) > 0;
                };
    }

    // returns true if date1 (with pattern date1Pattern) >= date 2 (with pattern date2Pattern)
    public static UDF4<String, String, String, String, Boolean> isDateGeqOtherDateUDF(){

        return (UDF4<String, String, String, String, Boolean>)
                (date1, date1Pattern, date2, date2Pattern) -> {

                    LocalDate localDate1 = LocalDate.parse(date1, DateTimeFormatter.ofPattern(date1Pattern));
                    LocalDate localDate2 = LocalDate.parse(date2, DateTimeFormatter.ofPattern(date2Pattern));
                    return localDate1.compareTo(localDate2) >= 0;
                };
    }

    // returns true if date1 (with pattern date1Pattern) < date 2 (with pattern date2Pattern)
    public static UDF4<String, String, String, String, Boolean> isDateLtOtherDateUDF(){

        return (UDF4<String, String, String, String, Boolean>)
                (date1, date1Pattern, date2, date2Pattern) -> {

                    LocalDate localDate1 = LocalDate.parse(date1, DateTimeFormatter.ofPattern(date1Pattern));
                    LocalDate localDate2 = LocalDate.parse(date2, DateTimeFormatter.ofPattern(date2Pattern));
                    return localDate1.compareTo(localDate2) < 0;
                };
    }

    // returns true if date1 (with pattern date1Pattern) <= date 2 (with pattern date2Pattern)
    public static UDF4<String, String, String, String, Boolean> isDateLeqOtherDateUDF(){

        return (UDF4<String, String, String, String, Boolean>)
                (date1, date1Pattern, date2, date2Pattern) -> {

                    LocalDate localDate1 = LocalDate.parse(date1, DateTimeFormatter.ofPattern(date1Pattern));
                    LocalDate localDate2 = LocalDate.parse(date2, DateTimeFormatter.ofPattern(date2Pattern));
                    return localDate1.compareTo(localDate2) <= 0;
        };
    }

    // returns true if stringDate (with pattern datePattern) is between
    // stringLowerDate (with pattern lowerDatePattern) and stringUpperDate (with pattern upperDatePattern)
    public static UDF6<String, String, String, String, String, String, Boolean> isDateBetweenLowerDateAndUpperDateUDF(){

        return (UDF6<String, String, String, String, String, String, Boolean>)
                (stringDate, datePattern, stringLowerDate, lowerDatePattern, stringUpperDate, upperDatePattern)-> {

                    LocalDate localDate = LocalDate.parse(stringDate, DateTimeFormatter.ofPattern(datePattern));
                    LocalDate lowerDate = LocalDate.parse(stringLowerDate, DateTimeFormatter.ofPattern(lowerDatePattern));
                    LocalDate upperDate = LocalDate.parse(stringUpperDate, DateTimeFormatter.ofPattern(upperDatePattern));
                    return (localDate.compareTo(lowerDate) >= 0) & (localDate.compareTo(upperDate) <= 0);
                };
    }

    // return the least date between two dates with same pattern
    public static UDF3<String, String, String, String> leastDateUDF(){

        return (UDF3<String, String, String, String>)
                (stringFirstDate, stringSecondDate, commonPattern) -> {

                    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(commonPattern);
                    LocalDate firstDate = LocalDate.parse(stringFirstDate, dateTimeFormatter);
                    LocalDate secondDate = LocalDate.parse(stringSecondDate, dateTimeFormatter);
                    return firstDate.compareTo(secondDate) <= 0 ? firstDate.format(dateTimeFormatter) : secondDate.format(dateTimeFormatter);
                };
    }

    // adds numberOfMonths to a date with pattern datePattern
    // equivalent of PIG function SubtractDuration(ToDate((chararray)datainiziodef,'yyyyMMdd'),'$numero_mesi_1')
    public static UDF3<String, String, Integer, String> substractDurationUDF(){

        return  (UDF3<String, String, Integer, String>)
                (date, datePattern, numberOfMonths) -> {

                    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(datePattern);
                    LocalDate localDate = LocalDate.parse(date, dateTimeFormatter);
                    return localDate.minusMonths(numberOfMonths).format(dateTimeFormatter);
                };
    }
}
