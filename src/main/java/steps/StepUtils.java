package steps;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

abstract class StepUtils {

    StepUtils(){}

    // convert column of dataset into given dataType
    Column castCol(Dataset<Row> df, String columnName, DataType dataType){
        return castCol(df.col(columnName), dataType);
    }

    private Column castCol(Column column, DataType dataType){
        return column.cast(dataType);
    }

    // convert a string column representing a date from the given input format to the given output date format
    Column convertStringColToDateCol(Column col, String inputDateFormat, String outputDateFormat){

        return functions.from_unixtime(functions.unix_timestamp(col, inputDateFormat), outputDateFormat);
    }

    // create a schema with one String column for each name provided
    StructType getDfSchema(List<String> columnNames){

        StructType schema = new StructType();
        for (String columnName: columnNames){

            schema = schema.add(new StructField(columnName, DataTypes.StringType, true, Metadata.empty()));

        }

        return schema;
    }

    // compute unixtimestamp for column of given dataset using the given format
    Column getUnixTimeStampCol(Dataset<Row> df, String columnName, String dateFormat){
        return functions.unix_timestamp(df.col(columnName), dateFormat);
    }

    // compute unixtimestamp for a generic column  using the given format
    Column getUnixTimeStampCol(Column column, String dateFormat){
        return functions.unix_timestamp(column, dateFormat);
    }

    // compute the greatest date between two date columns that have the same provided format
    Column greatestDate(Column dateColumn1, Column dateColumn2, String commonDateFormat){

        Column column1Ts = getUnixTimeStampCol(dateColumn1, commonDateFormat);
        Column column2Ts = getUnixTimeStampCol(dateColumn2, commonDateFormat);
        return functions.greatest(column1Ts, column2Ts);
    }

    // compute the greatest date between two date columns that have different format
    Column greatestDate(Column dateColumn1, Column dateColumn2, String date1Format, String date2Format){

        Column column1Ts = getUnixTimeStampCol(dateColumn1, date1Format);
        Column column2Ts = getUnixTimeStampCol(dateColumn2, date2Format);
        return functions.greatest(column1Ts, column2Ts);
    }

    // compute the least date between two date columns that have the same provided format
    Column leastDate(Column dateColumn1, Column dateColumn2, String commonDateFormat){

        Column column1Ts = getUnixTimeStampCol(dateColumn1, commonDateFormat);
        Column column2Ts = getUnixTimeStampCol(dateColumn2, commonDateFormat);
        return functions.least(column1Ts, column2Ts);
    }

    // compute the least date between two date columns that have different format
    Column leastDate(Column dateColumn1, Column dateColumn2, String date1Format, String date2Format){

        Column column1Ts = getUnixTimeStampCol(dateColumn1, date1Format);
        Column column2Ts = getUnixTimeStampCol(dateColumn2, date2Format);
        return functions.least(column1Ts, column2Ts);
    }

    // create a list of columns to be selected from the given dataset
    List<Column> selectDfColumns(Dataset<Row> df, List<String> columnNames){

        List<Column> dfCols = new ArrayList<Column>();
        for (String columnName: columnNames){
            dfCols.add(df.col(columnName));
        }

        return dfCols;

    }

    // create a list of columns to be selected from the given dataset, giving an alias to each column
    List<Column> selectDfColumns(Dataset<Row> df, Map<String,String> columnMap){

        List<Column> dfCols = new ArrayList<Column>();
        Set<Map.Entry<String, String>> entryList = columnMap.entrySet();
        for (Map.Entry<String, String> entry: entryList){

            dfCols.add(df.col(entry.getKey()).alias(entry.getValue()));
        }

        return dfCols;
    }
}

