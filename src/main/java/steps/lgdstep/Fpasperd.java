package steps.lgdstep;

import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.collection.Seq;
import steps.abstractstep.AbstractStep;
import steps.schemas.FpasperdSchema;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static steps.abstractstep.StepUtils.*;

public class Fpasperd extends AbstractStep {

    public Fpasperd(){

        logger = Logger.getLogger(Fpasperd.class);

        stepInputDir = getLGDPropertyValue("fpasperd.input.dir");
        stepOutputDir = getLGDPropertyValue("fpasperd.output.dir");

        logger.debug("stepInputDir: " + stepInputDir);
        logger.debug("stepOutputDir: " + stepOutputDir);
    }

    @Override
    public void run() {

        String cicliNdgPathCsv = getLGDPropertyValue("fpasperd.cicli.ndg.path.csv");
        String tlbpaspeCsv = getLGDPropertyValue("fpasperd.tlbpaspe.filter.csv");
        String tlbpaspeossCsv = getLGDPropertyValue("fpasperd.tlbpaspeoss.csv");
        String paspePaspeossGenDistCsv = getLGDPropertyValue("fpasperd.paspe.paspeoss.gen.dist.csv");

        logger.debug("cicliNdgPathCsv: " + cicliNdgPathCsv);
        logger.debug("tlbpaspeCsv:" + tlbpaspeCsv);
        logger.debug("tlbpaspeossCsv: " + tlbpaspeossCsv);
        logger.debug("paspePaspeossGenDistCsv: " + paspePaspeossGenDistCsv);

        // 19
        Map<String, String> tlbcidefLoadPigSchema = FpasperdSchema.getTlbcidefLoadPigSchema();
        StructType tlbcidefLoadStructType = fromPigSchemaToStructType(tlbcidefLoadPigSchema);
        Dataset<Row> tlbcidefLoad = sparkSession.read().format(csvFormat).option("delimiter", ",")
                .schema(tlbcidefLoadStructType).csv(cicliNdgPathCsv);

        // // (int)ToString(AddDuration( ToDate( (chararray)datafinedef,'yyyyMMdd' ),'P2M' ),'yyyyMMdd' )	AS  datafinedef
        // tlbcidef::datafinedef in format "yyyyMMdd"
        Column dataFineDefCol = addDuration(tlbcidefLoad.col("datafinedef"), "yyyyMMdd", 2).as("datafinedef");

        Dataset<Row> tlbcidef = tlbcidefLoad.select(functions.col("codicebanca"), functions.col("ndgprincipale"),
                functions.col("datainiziodef"), dataFineDefCol, functions.col("codicebanca_collegato"),
                functions.col("ndg_collegato"));
        // 56

        // 63
        Map<String, String> tlbpaspeFilterPigSchema = FpasperdSchema.getTlbpaspeFilterPigSchema();
        StructType tlbpaspeFilterStructType = fromPigSchemaToStructType(tlbpaspeFilterPigSchema);
        Dataset<Row> tlbpaspeFilter = sparkSession.read().format(csvFormat).option("delimiter", ",")
                .schema(tlbpaspeFilterStructType).csv(tlbpaspeCsv);

        // 71

        // 77

        // JOIN tlbpaspe_filter BY (cd_istituto, ndg) LEFT, tlbcidef BY (codicebanca_collegato, ndg_collegato);
        Column fpasperdBetweenGenJoinCondition = tlbpaspeFilter.col("cd_istituto").equalTo(tlbcidef.col("codicebanca_collegato"))
                .and(tlbpaspeFilter.col("ndg").equalTo(tlbcidef.col("ndg_collegato")));

        // BY (int)SUBSTRING((chararray)tlbpaspe_filter::datacont,0,6) >= (int)SUBSTRING((chararray)tlbcidef::datainiziodef,0,6)
        Column fpasperdBetweenGenDataContDataInizioDefFilterCol = substringAndCastToInt(tlbpaspeFilter.col("datacont"), 0, 6)
                .geq(substringAndCastToInt(tlbcidef.col("datainiziodef"), 0, 6));

        // AND (int)SUBSTRING((chararray)tlbpaspe_filter::datacont,0,6) < (int)SUBSTRING( (chararray)tlbcidef::datafinedef,0,6 )
        Column fpasperdBetweenGenDataContDataFineDefFilterCol = substringAndCastToInt(tlbpaspeFilter.col("datacont"), 0, 6)
                .lt(substringAndCastToInt(tlbcidef.col("datafinedef"), 0, 6));

        // DaysBetween( ToDate((chararray)tlbcidef::datafinedef,'yyyyMMdd' ), ToDate((chararray)tlbpaspe_filter::datacont,'yyyyMMdd' ) ) as days_diff
        Column fpasperdBetweenGenDaysDiffColl = daysBetween(tlbcidef.col("datafinedef"), tlbpaspeFilter.col("datacont"), "yyyyMMdd");

        // list of columns to be selected from dataframe tlbpaspeFilter
        List<String> tlbpaspeFilterSelectCols = Arrays.asList("cd_istituto", "ndg", "datacont", "causale", "importo");
        List<Column> fpasperdBetweenGenSelectColList = selectDfColumns(tlbpaspeFilter, tlbpaspeFilterSelectCols);

        // list of columns to be selected from dataframe tlbcidef
        List<String> tlbcidefSelectCols = Arrays.asList("codicebanca", "ndgprincipale", "datainiziodef", "datafinedef");
        fpasperdBetweenGenSelectColList.addAll(selectDfColumns(tlbcidef, tlbcidefSelectCols));

        Seq<Column> fpasperdBetweenGenSelectColsSeq = toScalaColSeq(fpasperdBetweenGenSelectColList);

        Dataset<Row> tlbcidefTlbpaspeFilterJoin = tlbpaspeFilter.join(tlbcidef, fpasperdBetweenGenJoinCondition, "left");
        Dataset<Row> fpasperdBetweenGen = tlbpaspeFilter.join(tlbcidef, fpasperdBetweenGenJoinCondition, "left")
                .filter(fpasperdBetweenGenDataContDataInizioDefFilterCol.and(fpasperdBetweenGenDataContDataFineDefFilterCol))
                .select(fpasperdBetweenGenSelectColsSeq).withColumn("days_diff", fpasperdBetweenGenDaysDiffColl);

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

        List<String> fpasperdOtherGenSelectColsNames = Arrays.asList("cd_istituto", "ndg", "datacont", "causale", "importo");
        List<Column> fpasperdOtherGenSelectColList = selectDfColumns(tlbpaspeFilter, fpasperdOtherGenSelectColsNames);

        fpasperdOtherGenSelectColList.add(functions.lit(null).cast(DataTypes.StringType).as("codicebanca"));
        fpasperdOtherGenSelectColList.add(functions.lit(null).cast(DataTypes.StringType).as("ndgprincipale"));
        fpasperdOtherGenSelectColList.add(functions.lit(null).cast(DataTypes.StringType).as("datainiziodef"));

        Seq<Column> fpasperdOtherGenSelectColsSeq = toScalaColSeq(fpasperdOtherGenSelectColList);
        Dataset<Row> fpasperdOtherGen = tlbcidefTlbpaspeFilterJoin.filter(tlbcidef.col("codicebanca").isNotNull())
                .select(fpasperdOtherGenSelectColsSeq);

        // 147

        // 152

        Seq<String> joinColumnsSeq = toScalaStringSeq(Arrays.asList("cd_istituto", "ndg", "datacont"));
        List<String> fpasperdOtherGenSelectColNames = Arrays.asList("cd_istituto", "ndg", "datacont", "causale",
                "importo", "codicebanca", "ndgprincipale", "datainiziodef");
        List<Column> fpasperdOtherOutSelectColList = selectDfColumns(fpasperdOtherGen, fpasperdOtherGenSelectColNames);
        Seq<Column> fpasperdOtherOutSelectColsSeq = toScalaColSeq(fpasperdOtherOutSelectColList);

        Dataset<Row> fpasperdOtherOut = fpasperdOtherGen.join(fpasperdBetweenOut, joinColumnsSeq, "left")
                .filter(fpasperdBetweenOut.col("cd_istituto").isNull()).select(fpasperdOtherOutSelectColsSeq);

        // 170

        // 175

        Dataset<Row> fpasperdNullOut = tlbcidefTlbpaspeFilterJoin
                .filter(tlbcidef.col("codicebanca").isNull())
                .select(fpasperdOtherGenSelectColsSeq);

        // 190

        // 198

        // JOIN fpasperd_null_out BY (cd_istituto, ndg) LEFT, tlbcidef BY (codicebanca, ndgprincipale);
        Column principFpasperdBetweenGenJoinCondition = fpasperdNullOut.col("cd_istituto").equalTo(tlbcidef.col("codicebanca"))
                .and(fpasperdNullOut.col("ndg").equalTo(tlbcidef.col("ndgprincipale")));

        //  BY (int)SUBSTRING((chararray)fpasperd_null_out::datacont,0,6) >= (int)SUBSTRING((chararray)tlbcidef::datainiziodef,0,6)
        Column principFpasperdBetweenGenDataContDataInizioDefFilterCol =
                substringAndCastToInt(fpasperdNullOut.col("datacont"), 0, 6)
                .geq(substringAndCastToInt(tlbcidef.col("datainiziodef"), 0, 6));

        // AND (int)SUBSTRING((chararray)fpasperd_null_out::datacont,0,6) < (int)SUBSTRING( (chararray)tlbcidef::datafinedef,0,6 )
        Column principFpasperdBetweenGenDataContDataFineDefFilterCol =
                substringAndCastToInt(fpasperdNullOut.col("datacont"), 0, 6)
                .lt(substringAndCastToInt(tlbcidef.col("datafinedef"), 0, 6));

        // DaysBetween( ToDate((chararray)tlbcidef::datafinedef,'yyyyMMdd' ),
        // ToDate((chararray)fpasperd_null_out::datacont,'yyyyMMdd' ) ) as days_diff
        Column principFpasperdBetweenGenDaysDiffColl =
                daysBetween(tlbcidef.col("datafinedef"), fpasperdNullOut.col("datacont"), "yyyyMMdd");

        // columns to be selected from dataframe fpasperdNullOut
        List<String> fpasperdNullOutSelectColNames = Arrays.asList("cd_istituto", "ndg", "datacont", "causale", "importo");
        List<Column> principFpasperdBetweenGenCols = selectDfColumns(fpasperdNullOut, fpasperdNullOutSelectColNames);

        // add columns to be selected from tlbcidef
        principFpasperdBetweenGenCols.addAll(selectDfColumns(tlbcidef, tlbcidefSelectCols));
        Seq<Column> principFpasperdBetweenGenColsSeq = toScalaColSeq(principFpasperdBetweenGenCols);

        Dataset<Row> principFpasperdBetweenGen = fpasperdNullOut.join(tlbcidef, principFpasperdBetweenGenJoinCondition, "left")
                .filter(principFpasperdBetweenGenDataContDataInizioDefFilterCol.and(principFpasperdBetweenGenDataContDataFineDefFilterCol))
                .select(principFpasperdBetweenGenColsSeq).withColumn("days_diff", principFpasperdBetweenGenDaysDiffColl);

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
        Column principFpasperdOtherGenJoinCondition =
                fpasperdNullOut.col("cd_istituto").equalTo(tlbcidef.col("codicebanca"))
                .and(fpasperdNullOut.col("ndg").equalTo(tlbcidef.col("ndgprincipale")));

        List<Column> principFpasperdOtherGenSelectColList = selectDfColumns(fpasperdNullOut, fpasperdNullOutSelectColNames);
        principFpasperdOtherGenSelectColList.add(functions.lit(null).cast(DataTypes.StringType).as("codicebanca"));
        principFpasperdOtherGenSelectColList.add(functions.lit(null).cast(DataTypes.StringType).as("ndgprincipale"));
        principFpasperdOtherGenSelectColList.add(functions.lit(null).cast(DataTypes.StringType).as("datainiziodef"));

        Seq<Column> principFpasperdOtherGenSelectColsSeq = toScalaColSeq(principFpasperdOtherGenSelectColList);
        Dataset<Row> principFpasperdOtherGen = fpasperdNullOut.join(tlbcidef, principFpasperdOtherGenJoinCondition, "left")
                .filter(tlbcidef.col("codicebanca").isNotNull())
                .select(principFpasperdOtherGenSelectColsSeq);

        // 265

        // 270

        List<String> principFpasperdOtherOutColNames =
                Arrays.asList("cd_istituto", "ndg", "datacont", "causale",
                        "importo", "codicebanca", "ndgprincipale", "datainiziodef");
        List<Column> principFpasperdOtherGenSelectCols = selectDfColumns(principFpasperdOtherGen,
                principFpasperdOtherOutColNames);

        Seq<Column> principFpasperdOtherOutSelectColsSeq = toScalaColSeq(principFpasperdOtherGenSelectCols);
        Dataset<Row> principFpasperdOtherOut = principFpasperdOtherGen.join(principFpasperdBetweenOut, joinColumnsSeq, "left")
                .filter(principFpasperdBetweenOut.col("cd_istituto").isNull())
                .select(principFpasperdOtherOutSelectColsSeq);

        // 288

        // 293
        Column principFpasperdNullOutJoinCondition = fpasperdNullOut.col("cd_istituto").equalTo(tlbcidef.col("codicebanca"))
                .and(fpasperdNullOut.col("ndg").equalTo(tlbcidef.col("ndgprincipale")));

        List<Column> principFpasperdNullOutCols = selectDfColumns(fpasperdNullOut, fpasperdNullOutSelectColNames);
        principFpasperdNullOutCols.add(functions.lit(null).cast(DataTypes.StringType).as("codicebanca"));
        principFpasperdNullOutCols.add(functions.lit(null).cast(DataTypes.StringType).as("ndgprincipale"));
        principFpasperdNullOutCols.add(functions.lit(null).cast(DataTypes.StringType).as("datainiziodef"));

        Seq<Column> principFpasperdNullOutSelectColsSeq = toScalaColSeq(principFpasperdNullOutCols);
        Dataset<Row> principFpasperdNullOut = fpasperdNullOut.join(tlbcidef, principFpasperdNullOutJoinCondition, "left")
                .filter(tlbcidef.col("codicebanca").isNull())
                .select(principFpasperdNullOutSelectColsSeq);

        // 308

        // 313

        Dataset<Row> fpasperdOutDistinct = fpasperdBetweenOut.union(fpasperdOtherOut.union(principFpasperdBetweenOut
                .union(principFpasperdOtherOut.union(principFpasperdNullOut)))).distinct();

        // 331

        // 336

        Map<String, String> tlbpaspeossPigSchema = FpasperdSchema.getTlbpaspeossPigSchema();
        StructType tlbpaspeossStructype = fromPigSchemaToStructType(tlbpaspeossPigSchema);
        Dataset<Row> tlbpaspeoss = sparkSession.read().format(csvFormat).option("delimiter", ",")
                .schema(tlbpaspeossStructype).csv(tlbpaspeossCsv);
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

        Column paspePaspeossGenDistJoinCondition = fpasperdOutDistinct.col("cd_istituto").equalTo(tlbpaspeoss.col("_cd_istituto"))
                .and(fpasperdOutDistinct.col("ndg").equalTo(tlbpaspeoss.col("_ndg")))
                .and(fpasperdOutDistinct.col("datacont").equalTo(tlbpaspeoss.col("_datacont")));

        Dataset<Row> paspePaspeossGenDist = fpasperdOutDistinct.join(tlbpaspeoss, paspePaspeossGenDistJoinCondition, "full_outer")
                .select(cdIstitutoCol, ndgCol, dataContCol, causaleCol, importoCol, codiceBancaCol, ndgPrincipaleCol, dataInizioDefCol);

        paspePaspeossGenDist.write().format(csvFormat).option("delimiter", ",").mode(SaveMode.Overwrite).csv(paspePaspeossGenDistCsv);

    }
}
