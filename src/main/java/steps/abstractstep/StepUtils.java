package steps.abstractstep;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.*;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

abstract class StepUtils {

    StepUtils(){}

    // convert column of dataset into given dataType
    protected Column castCol(Dataset<Row> df, String columnName, DataType dataType){
        return castCol(df.col(columnName), dataType);
    }

    private Column castCol(Column column, DataType dataType){
        return column.cast(dataType);
    }

    // convert a string column representing a date from the given input format to the given output date format
    protected Column castToDateCol(Column col, String inputF, String outputF){

        return functions.from_unixtime(functions.unix_timestamp(col, inputF), outputF);
    }

    // create a schema with one String column for each name provided
    protected StructType getDfSchema(List<String> columnNames){

        StructType schema = new StructType();
        for (String columnName: columnNames){

            schema = schema.add(new StructField(columnName, DataTypes.StringType, true, Metadata.empty()));

        }

        return schema;
    }

    // compute unixtimestamp for column of given dataset using the given format
    protected Column getUnixTimeStampCol(Dataset<Row> df, String columnName, String dateFormat){
        return functions.unix_timestamp(df.col(columnName), dateFormat);
    }

    // compute unixtimestamp for a generic column  using the given format
    protected Column getUnixTimeStampCol(Column column, String dateFormat){
        return functions.unix_timestamp(column, dateFormat);
    }

    // compute the greatest date between two date columns that have the same provided format
    Column greatestDate(Column dateColumn1, Column dateColumn2, String commonDateFormat){

        Column column1Ts = getUnixTimeStampCol(dateColumn1, commonDateFormat);
        Column column2Ts = getUnixTimeStampCol(dateColumn2, commonDateFormat);
        return functions.greatest(column1Ts, column2Ts);
    }

    // compute the greatest date between two date columns that have different format
    protected Column greatestDate(Column dateColumn1, Column dateColumn2, String date1Format, String date2Format){

        Column column1Ts = getUnixTimeStampCol(dateColumn1, date1Format);
        Column column2Ts = getUnixTimeStampCol(dateColumn2, date2Format);
        return functions.greatest(column1Ts, column2Ts);
    }

    // compute the least date between two date columns that have the same provided format
    protected Column leastDate(Column dateColumn1, Column dateColumn2, String commonDateFormat){

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

    // replace oldString with newString and convert to Double column
    protected Column replaceAndConvertToDouble(Dataset<Row> df, String columnName, String oldString, String newString){

        return functions.regexp_replace(df.col(columnName), oldString, newString)
                .cast(DataTypes.DoubleType).as(columnName);
    }

    // create a list of columns to be selected from the given dataset
    protected List<Column> selectDfColumns(Dataset<Row> df, List<String> columnNames){

        List<Column> dfCols = new ArrayList<Column>();
        for (String columnName: columnNames){
            dfCols.add(df.col(columnName));
        }

        return dfCols;

    }

    // create a list of columns to be selected from the given dataset, giving an alias to each column
    protected List<Column> selectDfColumns(Dataset<Row> df, Map<String,String> columnMap){

        List<Column> dfCols = new ArrayList<Column>();
        Set<Map.Entry<String, String>> entryList = columnMap.entrySet();
        for (Map.Entry<String, String> entry: entryList){

            dfCols.add(df.col(entry.getKey()).alias(entry.getValue()));
        }

        return dfCols;
    }

    // convert a list of column expression into scala.collection.Seq of column expressions
    protected Seq<Column> toScalaSeq(List<Column> columnList){
        return JavaConverters.asScalaIteratorConverter(columnList.iterator()).asScala().toSeq();
    }

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

