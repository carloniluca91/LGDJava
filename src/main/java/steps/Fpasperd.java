package steps;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

public class Fpasperd extends AbstractStep{

    Fpasperd(String[] args){}

    @Override
    public void run() {

        // 19
        List<String> tlbcidefLoadColumns = Arrays.asList("codicebanca", "ndgprincipale", "datainiziodef", "datafinedef", "datainiziopd",
                "datainizioristrutt", "datainizioinc", "datainiziosoff", "c_key", "tipo_segmne", "sae_segm", "rae_segm",
                "segmento", "tp_ndg", "provincia_segm", "databilseg", "strbilseg", "attivobilseg", "fatturbilseg",
                "ndg_collegato", "codicebanca_collegato", "cd_collegamento", "cd_fiscale");

        StructType tlbcidefLoadSchema = getDfSchema(tlbcidefLoadColumns);

        String fpasPerdInputDir = getProperty("FPASPERD_INPUT_DIR");
        String cicliNdgPathCsv = getProperty("CICLI_NDG_PATH_CSV");
        logger.info("fpasPerdInputDir: " + fpasPerdInputDir);
        logger.info("cicliNdgPathCsv: " + cicliNdgPathCsv);

        String tlbcidefLoadPath = Paths.get(fpasPerdInputDir, cicliNdgPathCsv).toString();
        String csvFormat = getProperty("csv_format");
        logger.info("tlbcidefLoadPath: " + tlbcidefLoadPath);
        logger.info("csvFormat: " + csvFormat);

        Dataset<Row> tlbcidefLoad = sparkSession.read().format(csvFormat).option("delimiter", ",").schema(
                tlbcidefLoadSchema).csv(tlbcidefLoadPath);

        // datafinedef in format "yyyy-MM-dd"
        Column dataFineDefDateCol = convertStringColToDateCol(
                tlbcidefLoad.col("datafinedef"), "yyyyMMdd", "yyyy-MM-dd");

        Dataset<Row> tlbcidef = tlbcidefLoad.select(functions.col("codicebanca"), functions.col("ndgprincipale"),
                functions.col("datainiziodef"), functions.add_months(dataFineDefDateCol, 2).as("datafinedef"),
                functions.col("codicebanca_collegato"), functions.col("ndg_collegato"));
        // 56

        // 63
        List<String> tlbpaspeColumns = Arrays.asList("cd_istituto", "ndg", "datacont", "causale", "importo");
        StructType tlbpaspeSchema = getDfSchema(tlbpaspeColumns);

        String tlbpaspeCsv = getProperty("TLBPASPE_CSV");
        String tlbpaspeCsvPath = Paths.get(fpasPerdInputDir, tlbpaspeCsv).toString();
        logger.info("tlbpaspeCsv:" + tlbpaspeCsv);
        logger.info("tlbpaspeCsvPath: " + tlbpaspeCsvPath);

        Dataset<Row> tlbpaspeFilter = sparkSession.read().format(csvFormat).option("delimiter", ",").schema(
                tlbpaspeSchema).csv(tlbpaspeCsvPath);

        // 71

        // 77

        Column joinCondition = tlbpaspeFilter.col("cd_istituto").equalTo(tlbcidef.col("codicebanca_collegato"))
                .and(tlbpaspeFilter.col("ndg").equalTo(tlbcidef.col("ndg_collegato")));

        Column dataContDataInizioDefFilterCol = getUnixTimeStampCol(
                functions.substring(tlbpaspeFilter.col("datacont"), 0, 6), "yyyyMM").$greater$eq(
                        getUnixTimeStampCol(functions.substring(tlbcidef.col("datainiziodef"), 0, 6),
                                "yyyyMM"));

        Column dataContDataFineDefFIlterCol = getUnixTimeStampCol(
                functions.substring(tlbpaspeFilter.col("datacont"), 0, 6), "yyyyMM").$less(
                getUnixTimeStampCol(functions.substring(tlbcidef.col("datafinedef"), 0, 7),
                        "yyyy-MM"));

        Column daysDiffColl = functions.datediff(tlbcidef.col("datafinedef"), convertStringColToDateCol(
                tlbpaspeFilter.col("datacont"), "yyyyMMdd", "yyyy-MM-dd"))
                .as("days_diff");

        // list of columns to be selected from dataframe tlbpaspeFilter
        List<String> tlbpaspeFilterSelectCols = Arrays.asList("cd_istituto", "ndg", "datacont", "causale", "importo");
        logger.info("tlpaspeFilter columns to be selected: " + tlbpaspeFilterSelectCols.toString());
        List<Column> selectCols = selectDfColumns(tlbpaspeFilter, tlbpaspeFilterSelectCols);

        // list of columns to be selected from dataframe tlbcidef
        List<String> tlbcidefSelectCols = Arrays.asList("codicebanca", "ndgprincipale", "datainiziodef", "datafinedef");
        logger.info("tlbcidef columns to be selected: " + tlbcidefSelectCols.toString());
        selectCols.addAll(selectDfColumns(tlbcidef, tlbcidefSelectCols));

        // add days_diff column
        selectCols.add(daysDiffColl);

        // conversion to scala Seq
        Seq<Column> selectColsSeq = JavaConverters.asScalaIteratorConverter(selectCols.iterator()).asScala().toSeq();

        Dataset<Row> tlbcidefTlbpaspeFilterJoin = tlbpaspeFilter.join(tlbcidef, joinCondition, "left");
        Dataset<Row> fpasperdBetweenGen = tlbcidefTlbpaspeFilterJoin.filter(dataContDataInizioDefFilterCol.and(
                dataContDataFineDefFIlterCol)).select(selectColsSeq);

        // 104

        // 109

        Row top = fpasperdBetweenGen.orderBy(functions.col("days_diff")).groupBy().agg(
                functions.first("top_importo").as("importo"),
                functions.first("datainiziodef").as("datainiziodef")).collect()[0];

        int topImporto = top.getAs("top_importo");
        String topDataInizioDef = top.getAs("datainiziodef");
        logger.info("topImporto: " + topImporto);
        logger.info("topDataInizioDef: " + topDataInizioDef);

        Dataset<Row> fpasperdBetweenOut = fpasperdBetweenGen.select(functions.col("cd_istituto"),
                functions.col("ndg"), functions.col("datacont"), functions.col("causale"),
                functions.lit(topImporto).as("importo"), functions.col("codicebanca"),
                functions.col("ndgprincipale"), functions.lit(topDataInizioDef).as("datainiziodef"));
        // 127

        // 132

        tlbpaspeFilterSelectCols = Arrays.asList("cd_istituto", "ndg", "datacont", "causale", "importo");
        selectCols = selectDfColumns(tlbpaspeFilter, tlbpaspeFilterSelectCols);

        selectCols.add(functions.lit(null).as("codicebanca"));
        selectCols.add(functions.lit(null).as("ndgprincipale"));
        selectCols.add(functions.lit(null).as("datainiziodef"));

        // conversion to scala Seq
        selectColsSeq = JavaConverters.asScalaIteratorConverter(selectCols.iterator()).asScala().toSeq();
        Dataset<Row> fpasperdOtherGen = tlbcidefTlbpaspeFilterJoin.filter(tlbcidef.col("codicebanca").isNotNull())
                .select(selectColsSeq);

        // 147

        // 152

        List<String> joinColumns = Arrays.asList("cd_istituto", "ndg", "datacont");
        Seq<String> joinColumnsSeq = JavaConverters.asScalaIteratorConverter(joinColumns.iterator()).asScala().toSeq();

        List<String> fpasperdOtherGenSelectColNames = Arrays.asList("cd_istituto", "ndg", "datacont", "causale",
                "importo", "codicebanca", "ndgprincipale", "datainiziodef");
        List<Column> fpasperdOtherGenSelectCols = selectDfColumns(fpasperdOtherGen, fpasperdOtherGenSelectColNames);
        Seq<Column> fpasperdOtherGenSelectColsSeq = JavaConverters.asScalaIteratorConverter(
                fpasperdOtherGenSelectCols.iterator()).asScala().toSeq();

        Dataset<Row> fpasperdOtherOut = fpasperdOtherGen.join(fpasperdBetweenOut, joinColumnsSeq, "left")
                .filter(fpasperdBetweenOut.col("cd_istituto").isNull()).select(fpasperdOtherGenSelectColsSeq);

        // 170

        // 175

        Dataset<Row> fpasperdNullOut = tlbcidefTlbpaspeFilterJoin.filter(
                tlbcidef.col("codicebanca").isNull()).select(selectColsSeq);

        // 190

        // 198

        joinCondition = fpasperdNullOut.col("cd_istituto").equalTo(tlbcidef.col("codicebanca"))
                .and(fpasperdNullOut.col("ndg").equalTo(tlbcidef.col("ndgprincipale")));

        dataContDataInizioDefFilterCol = getUnixTimeStampCol(functions.substring(
                fpasperdNullOut.col("datacont"), 0, 6), "yyyyMM").$greater$eq(
                        getUnixTimeStampCol(functions.substring(tlbcidef.col("datainiziodef"), 0, 6),
                                "yyyyMM"));

        dataContDataFineDefFIlterCol = getUnixTimeStampCol(functions.substring(
                fpasperdNullOut.col("datacont"), 0, 6), "yyyyMM").$less(
                        getUnixTimeStampCol(functions.substring(tlbcidef.col("datafinedef"), 0, 7),
                                "yyyy-MM"));

        daysDiffColl = functions.datediff(tlbcidef.col("datafinedef"), convertStringColToDateCol(
                fpasperdNullOut.col("datacont"), "yyyyMMdd", "yyyy-MM-dd"))
                .as("days_diff");

        // columns to be selected from dataframe fpasperdNullOut
        List<String> fpasperdNullOutSelectColNames = Arrays.asList("cd_istituto", "ndg", "datacont", "causale", "importo");
        List<Column> principFpasperdBetweenGenCols = selectDfColumns(fpasperdNullOut, fpasperdNullOutSelectColNames);

        // add columns to be selected from tlbcidef as wel as days_diff column
        principFpasperdBetweenGenCols.addAll(selectDfColumns(tlbcidef, tlbcidefSelectCols));
        principFpasperdBetweenGenCols.add(daysDiffColl);

        // conversion to scala Seq
        Seq<Column> principFpasperdBetweenGenColsSeq = JavaConverters.asScalaIteratorConverter(principFpasperdBetweenGenCols
                .iterator()).asScala().toSeq();

        Dataset<Row> principFpasperdBetweenGen = fpasperdNullOut.join(tlbcidef, joinCondition).filter(
                dataContDataInizioDefFilterCol.and(dataContDataFineDefFIlterCol)).select(principFpasperdBetweenGenColsSeq);

        // 222

        // 227
        // TODO
        // 245


    }
}
