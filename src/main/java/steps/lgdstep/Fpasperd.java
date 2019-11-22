package steps.lgdstep;

import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import steps.abstractstep.AbstractStep;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

public class Fpasperd extends AbstractStep {

    public Fpasperd(String loggerName){

        super(loggerName);
        logger = Logger.getLogger(loggerName);

        stepInputDir = getPropertyValue("fpasperd.input.dir");
        stepOutputDir = getPropertyValue("fpasperd.output.dir");

        logger.debug("stepInputDir: " + stepInputDir);
        logger.debug("stepOutputDir: " + stepOutputDir);
    }

    @Override
    public void run() {

        // 19
        List<String> tlbcidefLoadColumns = Arrays.asList("codicebanca", "ndgprincipale", "datainiziodef", "datafinedef", "datainiziopd",
                "datainizioristrutt", "datainizioinc", "datainiziosoff", "c_key", "tipo_segmne", "sae_segm", "rae_segm",
                "segmento", "tp_ndg", "provincia_segm", "databilseg", "strbilseg", "attivobilseg", "fatturbilseg",
                "ndg_collegato", "codicebanca_collegato", "cd_collegamento", "cd_fiscale");

        StructType tlbcidefLoadSchema = getStringTypeSchema(tlbcidefLoadColumns);

        String cicliNdgPathCsv = getPropertyValue("cicli.ndg.path.csv");
        String tlbcidefLoadPath = Paths.get(stepInputDir, cicliNdgPathCsv).toString();
        String csvFormat = getPropertyValue("csv.format");

        logger.debug("cicliNdgPathCsv: " + cicliNdgPathCsv);
        logger.debug("tlbcidefLoadPath: " + tlbcidefLoadPath);
        logger.debug("csvFormat: " + csvFormat);

        Dataset<Row> tlbcidefLoad = sparkSession.read().format(csvFormat).option("delimiter", ",").schema(
                tlbcidefLoadSchema).csv(tlbcidefLoadPath);

        // // (int)ToString(AddDuration( ToDate( (chararray)datafinedef,'yyyyMMdd' ),'P2M' ),'yyyyMMdd' )	AS  datafinedef
        // tlbcidef::datafinedef in format "yyyyMMdd"
        Column dataFineDefCol = stringDateFormat(tlbcidefLoad.col("datafinedef"),
                "yyyyMMdd", "yyyy-MM-dd");
        dataFineDefCol = functions.date_format(functions.add_months(dataFineDefCol, 2), "yyyyMMdd").as("datafinedef");

        Dataset<Row> tlbcidef = tlbcidefLoad.select(functions.col("codicebanca"), functions.col("ndgprincipale"),
                functions.col("datainiziodef"), dataFineDefCol, functions.col("codicebanca_collegato"),
                functions.col("ndg_collegato"));
        // 56

        // 63
        List<String> tlbpaspeColumns = Arrays.asList("cd_istituto", "ndg", "datacont", "causale", "importo");
        StructType tlbpaspeSchema = getStringTypeSchema(tlbpaspeColumns);

        String tlbpaspeCsv = getPropertyValue("tlbpaspe.filter.csv");
        String tlbpaspeCsvPath = Paths.get(stepInputDir, tlbpaspeCsv).toString();
        logger.info("tlbpaspeCsv:" + tlbpaspeCsv);
        logger.info("tlbpaspeCsvPath: " + tlbpaspeCsvPath);

        Dataset<Row> tlbpaspeFilter = sparkSession.read().format(csvFormat).option("delimiter", ",").schema(
                tlbpaspeSchema).csv(tlbpaspeCsvPath);

        // 71

        // 77

        // JOIN tlbpaspe_filter BY (cd_istituto, ndg) LEFT, tlbcidef BY (codicebanca_collegato, ndg_collegato);
        Column joinCondition = tlbpaspeFilter.col("cd_istituto").equalTo(tlbcidef.col("codicebanca_collegato"))
                .and(tlbpaspeFilter.col("ndg").equalTo(tlbcidef.col("ndg_collegato")));

        // BY (int)SUBSTRING((chararray)tlbpaspe_filter::datacont,0,6) >= (int)SUBSTRING((chararray)tlbcidef::datainiziodef,0,6)
        Column dataContDataInizioDefFilterCol = getUnixTimeStampCol(
                functions.substring(tlbpaspeFilter.col("datacont"), 0, 6), "yyyyMM")
                .$greater$eq(getUnixTimeStampCol(functions.substring(tlbcidef.col("datainiziodef"), 0, 6), "yyyyMM"));

        // AND (int)SUBSTRING((chararray)tlbpaspe_filter::datacont,0,6) < (int)SUBSTRING( (chararray)tlbcidef::datafinedef,0,6 )
        Column dataContDataFineDefFIlterCol = getUnixTimeStampCol(
                functions.substring(tlbpaspeFilter.col("datacont"), 0, 6), "yyyyMM")
                .$less(getUnixTimeStampCol(functions.substring(tlbcidef.col("datafinedef"), 0, 6), "yyyyMM"));

        // DaysBetween( ToDate((chararray)tlbcidef::datafinedef,'yyyyMMdd' ), ToDate((chararray)tlbpaspe_filter::datacont,'yyyyMMdd' ) ) as days_diff
        Column daysDiffColl = functions.datediff(
                stringDateFormat(tlbcidef.col("datafinedef"), "yyyyMMdd", "yyyy-MM-dd"),
                stringDateFormat(tlbpaspeFilter.col("datacont"), "yyyyMMdd", "yyyy-MM-dd"));

        // list of columns to be selected from dataframe tlbpaspeFilter
        List<String> tlbpaspeFilterSelectCols = Arrays.asList("cd_istituto", "ndg", "datacont", "causale", "importo");
        logger.info("tlpaspeFilter columns to be selected: " + tlbpaspeFilterSelectCols.toString());
        List<Column> selectCols = selectDfColumns(tlbpaspeFilter, tlbpaspeFilterSelectCols);

        // list of columns to be selected from dataframe tlbcidef
        List<String> tlbcidefSelectCols = Arrays.asList("codicebanca", "ndgprincipale", "datainiziodef", "datafinedef");
        logger.info("tlbcidef columns to be selected: " + tlbcidefSelectCols.toString());
        selectCols.addAll(selectDfColumns(tlbcidef, tlbcidefSelectCols));

        // conversion to scala Seq
        Seq<Column> selectColsSeq = JavaConverters.asScalaIteratorConverter(selectCols.iterator()).asScala().toSeq();

        Dataset<Row> tlbcidefTlbpaspeFilterJoin = tlbpaspeFilter.join(tlbcidef, joinCondition, "left");
        Dataset<Row> fpasperdBetweenGen = tlbpaspeFilter.join(tlbcidef, joinCondition, "left")
                .filter(dataContDataInizioDefFilterCol.and(dataContDataFineDefFIlterCol))
                .select(selectColsSeq).withColumn("days_diff", daysDiffColl);

        // 104

        // 109
        // GROUP fpasperd_between_gen BY ( cd_istituto, ndg, datacont, causale, codicebanca, ndgprincipale );
        // ... ORDER fpasperd_between_gen by days_diff ASC
        WindowSpec w = Window.partitionBy("cd_istituto", "ndg", "datacont", "causale", "codicebanca", "ndgprincipale")
                .orderBy("days_diff");

        Dataset<Row> fpasperdBetweenOut = fpasperdBetweenGen.select(functions.col("cd_istituto"),
                functions.col("ndg"), functions.col("datacont"), functions.col("causale"),
                functions.first("importo").over(w).as("importo"),
                functions.col("codicebanca"), functions.col("ndgprincipale"),
                functions.first("datainiziodef").over(w).as("datainiziodef"));
        // 127

        // 132

        tlbpaspeFilterSelectCols = Arrays.asList("cd_istituto", "ndg", "datacont", "causale", "importo");
        selectCols = selectDfColumns(tlbpaspeFilter, tlbpaspeFilterSelectCols);

        selectCols.add(functions.lit(null).as("codicebanca"));
        selectCols.add(functions.lit(null).as("ndgprincipale"));
        selectCols.add(functions.lit(null).as("datainiziodef"));

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

        // JOIN fpasperd_null_out BY (cd_istituto, ndg) LEFT, tlbcidef BY (codicebanca, ndgprincipale);
        joinCondition = fpasperdNullOut.col("cd_istituto").equalTo(tlbcidef.col("codicebanca"))
                .and(fpasperdNullOut.col("ndg").equalTo(tlbcidef.col("ndgprincipale")));

        //  BY (int)SUBSTRING((chararray)fpasperd_null_out::datacont,0,6) >= (int)SUBSTRING((chararray)tlbcidef::datainiziodef,0,6)
        dataContDataInizioDefFilterCol = getUnixTimeStampCol(functions.substring(
                fpasperdNullOut.col("datacont"), 0, 6), "yyyyMM").$greater$eq(
                        getUnixTimeStampCol(functions.substring(tlbcidef.col("datainiziodef"), 0, 6),
                                "yyyyMM"));

        // AND (int)SUBSTRING((chararray)fpasperd_null_out::datacont,0,6) < (int)SUBSTRING( (chararray)tlbcidef::datafinedef,0,6 )
        dataContDataFineDefFIlterCol = getUnixTimeStampCol(functions.substring(
                fpasperdNullOut.col("datacont"), 0, 6), "yyyyMM").$less(
                        getUnixTimeStampCol(functions.substring(tlbcidef.col("datafinedef"), 0, 6),
                                "yyyyMM"));

        // DaysBetween( ToDate((chararray)tlbcidef::datafinedef,'yyyyMMdd' ), ToDate((chararray)fpasperd_null_out::datacont,'yyyyMMdd' ) ) as days_diff
        daysDiffColl = functions.datediff(
                stringDateFormat(tlbcidef.col("datafinedef"), "yyyyMMdd", "yyyy-MM-dd"),
                stringDateFormat(fpasperdNullOut.col("datacont"), "yyyyMMdd", "yyyy-MM-dd"));

        // columns to be selected from dataframe fpasperdNullOut
        List<String> fpasperdNullOutSelectColNames = Arrays.asList("cd_istituto", "ndg", "datacont", "causale", "importo");
        List<Column> principFpasperdBetweenGenCols = selectDfColumns(fpasperdNullOut, fpasperdNullOutSelectColNames);

        // add columns to be selected from tlbcidef
        principFpasperdBetweenGenCols.addAll(selectDfColumns(tlbcidef, tlbcidefSelectCols));
        Seq<Column> principFpasperdBetweenGenColsSeq = JavaConverters.asScalaIteratorConverter(principFpasperdBetweenGenCols
                .iterator()).asScala().toSeq();

        Dataset<Row> principFpasperdBetweenGen = fpasperdNullOut.join(tlbcidef, joinCondition, "left")
                .filter(dataContDataInizioDefFilterCol.and(dataContDataFineDefFIlterCol))
                .select(principFpasperdBetweenGenColsSeq).withColumn("days_diff", daysDiffColl);

        // 222

        // 227
        Dataset<Row> principFpasperdBetweenOut = principFpasperdBetweenGen.select(functions.col("cd_istituto"),
                functions.col("ndg"), functions.col("datacont"), functions.col("causale"),
                functions.first("importo").over(w).as("importo"),
                functions.col("codicebanca"), functions.col("ndgprincipale"),
                functions.first("datainiziodef").over(w).as("datainiziodef"));

        // 245

        // 250
        // JOIN fpasperd_null_out BY (cd_istituto, ndg) LEFT, tlbcidef BY (codicebanca, ndgprincipale);
        joinCondition = fpasperdNullOut.col("cd_istituto").equalTo(tlbcidef.col("codicebanca"))
                .and(fpasperdNullOut.col("ndg").equalTo(tlbcidef.col("ndgprincipale")));

        selectCols = selectDfColumns(fpasperdNullOut, fpasperdNullOutSelectColNames);

        selectCols.add(functions.lit(null).as("codicebanca"));
        selectCols.add(functions.lit(null).as("ndgprincipale"));
        selectCols.add(functions.lit(null).as("datainiziodef"));

        selectColsSeq = JavaConverters.asScalaIteratorConverter(selectCols.iterator()).asScala().toSeq();
        Dataset<Row> principFpasperdOtherGen = fpasperdNullOut.join(tlbcidef, joinCondition, "left").filter(
                tlbcidef.col("codicebanca").isNotNull()).select(selectColsSeq);

        // 265

        // 270

        List<String> principFpasperdOtherOutColNames = Arrays.asList("cd_istituto", "ndg", "datacont", "causale",
                "importo", "codicebanca", "ndgprincipale", "datainiziodef");
        List<Column> principFpasperdOtherGenSelectCols = selectDfColumns(principFpasperdOtherGen,
                principFpasperdOtherOutColNames);

        selectColsSeq = JavaConverters.asScalaIteratorConverter(principFpasperdOtherGenSelectCols.iterator()).asScala().toSeq();
        Dataset<Row> principFpasperdOtherOut = principFpasperdOtherGen.join(principFpasperdBetweenOut, joinColumnsSeq, "left")
                .filter(principFpasperdBetweenOut.col("cd_istituto").isNull()).select(selectColsSeq);

        // 288

        // 293
        joinCondition = fpasperdNullOut.col("cd_istituto").equalTo(tlbcidef.col("codicebanca"))
                .and(fpasperdNullOut.col("ndg").equalTo(tlbcidef.col("ndgprincipale")));

        List<Column> principFpasperdNullOutCols = selectDfColumns(fpasperdNullOut, fpasperdNullOutSelectColNames);
        principFpasperdNullOutCols.add(functions.lit(null).as("codicebanca"));
        principFpasperdNullOutCols.add(functions.lit(null).as("ndgprincipale"));
        principFpasperdNullOutCols.add(functions.lit(null).as("datainiziodef"));

        selectColsSeq = JavaConverters.asScalaIteratorConverter(principFpasperdNullOutCols.iterator()).asScala().toSeq();
        Dataset<Row> principFpasperdNullOut = fpasperdNullOut.join(tlbcidef, joinCondition, "left").filter(
                tlbcidef.col("codicebanca").isNull()).select(selectColsSeq);

        // 308

        // 313

        Dataset<Row> fpasperdOutDistinct = fpasperdBetweenOut.union(fpasperdOtherOut.union(principFpasperdBetweenOut
                .union(principFpasperdOtherOut.union(principFpasperdNullOut)))).distinct();

        // 331

        // 336

        String tlbpaspeossCsv = getPropertyValue("tlbpaspeoss.csv");
        String tlbpaspeossCsvPath = Paths.get(stepInputDir, tlbpaspeossCsv).toString();
        logger.info("tlbpaspeossCsv: " + tlbpaspeossCsv);
        logger.info("tlbpaspeossCsvPath: " + tlbpaspeossCsvPath);

        // slightly change the field names for tlbpaspeoss in order to avoid implicit coalesce operator
        // triggered by performing "full_outer" join on columns with same name
        List<String> tlbpaspeossCols = Arrays.asList("_cd_istituto", "_ndg", "_datacont", "_causale", "_importo");
        StructType tlbpaspeossSchema = getStringTypeSchema(tlbpaspeossCols);
        Dataset<Row> tlbpaspeoss = sparkSession.read().format(csvFormat).option("delimiter", ",")
                .schema(tlbpaspeossSchema).csv(tlbpaspeossCsvPath);
        // 344

        // 346
        // ( fpasperd_out_distinct::cd_istituto is not null?
        //      ( tlbpaspeoss::cd_istituto is not null? tlbpaspeoss::cd_istituto : fpasperd_out_distinct::cd_istituto ):
        //      tlbpaspeoss::cd_istituto ) as cd_istituto
        Column cdIstitutoCol = functions.when(fpasperdOutDistinct.col("cd_istituto").isNotNull(),
                functions.coalesce(tlbpaspeoss.col("_cd_istituto"), fpasperdOutDistinct.col("cd_istituto")))
                .otherwise(tlbpaspeoss.col("_cd_istituto")).as("cd_istituto");

        // ( fpasperd_out_distinct::cd_istituto is not null?
        //      ( tlbpaspeoss::cd_istituto is not null? tlbpaspeoss::ndg : fpasperd_out_distinct::ndg ) :
        //      tlbpaspeoss::ndg ) as ndg
        Column ndgCol = functions.when(fpasperdOutDistinct.col("cd_istituto").isNotNull(),
                functions.coalesce(tlbpaspeoss.col("_ndg"), fpasperdOutDistinct.col("ndg")))
                .otherwise(tlbpaspeoss.col("_ndg")).as("ndg");

        // ( fpasperd_out_distinct::cd_istituto is not null?
        //      ( tlbpaspeoss::cd_istituto is not null? tlbpaspeoss::datacont : fpasperd_out_distinct::datacont ) :
        //      tlbpaspeoss::datacont ) as datacont
        Column dataContCol = functions.when(fpasperdOutDistinct.col("cd_istituto").isNotNull(),
                functions.coalesce(tlbpaspeoss.col("_datacont"), fpasperdOutDistinct.col("datacont")))
                .otherwise(tlbpaspeoss.col("_datacont")).as("datacont");

        // ( fpasperd_out_distinct::cd_istituto is not null?
        //      ( tlbpaspeoss::cd_istituto is not null? tlbpaspeoss::causale : fpasperd_out_distinct::causale ) :
        //      tlbpaspeoss::causale ) as causale
        Column causaleCol = functions.when(fpasperdOutDistinct.col("cd_istituto").isNotNull(),
                functions.coalesce(tlbpaspeoss.col("_causale"), fpasperdOutDistinct.col("causale")))
                .otherwise(tlbpaspeoss.col("_causale")).as("causale");

        // ( fpasperd_out_distinct::cd_istituto is not null?
        //      ( tlbpaspeoss::cd_istituto is not null? tlbpaspeoss::importo : fpasperd_out_distinct::importo ) :
        //      tlbpaspeoss::importo ) as importo
        Column importoCol = functions.when(fpasperdOutDistinct.col("cd_istituto").isNotNull(),
                functions.coalesce(tlbpaspeoss.col("_importo"),  fpasperdOutDistinct.col("importo")))
                .otherwise(tlbpaspeoss.col("_importo")).as("importo");

        // ( fpasperd_out_distinct::cd_istituto is not null? fpasperd_out_distinct::codicebanca : NULL ) as codicebanca
        Column codiceBancaCol = functions.when(fpasperdOutDistinct.col("cd_istituto").isNotNull(),
                fpasperdOutDistinct.col("codicebanca")).otherwise(null).as("codicebanca");

        // ( fpasperd_out_distinct::cd_istituto is not null? fpasperd_out_distinct::ndgprincipale : NULL ) as ndgprincipale
        Column ndgPrincipaleCol = functions.when(fpasperdOutDistinct.col("cd_istituto").isNotNull(),
                fpasperdOutDistinct.col("ndgprincipale")).otherwise(null).as("ndgprincipale");

        // ( fpasperd_out_distinct::cd_istituto is not null? fpasperd_out_distinct::datainiziodef : NULL ) as datainiziodef
        Column dataInizioDefCol = functions.when(fpasperdOutDistinct.col("cd_istituto").isNotNull(),
                fpasperdOutDistinct.col("datainiziodef")).otherwise(null).as("datainiziodef");

        joinCondition = fpasperdOutDistinct.col("cd_istituto").equalTo(tlbpaspeoss.col("_cd_istituto"))
                .and(fpasperdOutDistinct.col("ndg").equalTo(tlbpaspeoss.col("_ndg")))
                .and(fpasperdOutDistinct.col("datacont").equalTo(tlbpaspeoss.col("_datacont")));

        Dataset<Row> paspePaspeossGenDist = fpasperdOutDistinct.join(tlbpaspeoss, joinCondition, "full_outer")
                .select(cdIstitutoCol, ndgCol, dataContCol, causaleCol, importoCol, codiceBancaCol, ndgPrincipaleCol, dataInizioDefCol);

        String paspePaspeossGenDistCsv = getPropertyValue("paspe.paspeoss.gen.dist.csv");
        logger.info("paspePaspeossGenDistCsv: " + paspePaspeossGenDistCsv);

        String paspePaspeossGenDistCsvPath = Paths.get(stepOutputDir, paspePaspeossGenDistCsv).toString();
        logger.info("paspePaspeossGenDistCsvPath: " + paspePaspeossGenDistCsvPath);

        paspePaspeossGenDist.write().format(csvFormat).option("delimiter", ",").mode(SaveMode.Overwrite).csv(paspePaspeossGenDistCsvPath);
        // 379
    }
}
