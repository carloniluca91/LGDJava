package it.carloni.luca.lgd.spark.step;

import it.carloni.luca.lgd.parameter.step.EmptyValue;
import it.carloni.luca.lgd.spark.common.AbstractStep;
import it.carloni.luca.lgd.schema.FpasperdSchema;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import java.util.Arrays;

import static it.carloni.luca.lgd.spark.utils.StepUtils.addDurationUDF;
import static it.carloni.luca.lgd.spark.utils.StepUtils.daysBetweenUDF;
import static it.carloni.luca.lgd.spark.utils.StepUtils.substringAndToInt;
import static it.carloni.luca.lgd.spark.utils.StepUtils.toScalaSeq;
import static it.carloni.luca.lgd.spark.utils.StepUtils.toStringCol;

public class Fpasperd extends AbstractStep<EmptyValue> {

    private final Logger logger = Logger.getLogger(getClass());

    @Override
    public void run(EmptyValue emptyValues) {

        String cicliNdgPathCsv = getValue("fpasperd.cicli.ndg.path.csv");
        String tlbpaspeCsv = getValue("fpasperd.tlbpaspe.filter.csv");
        String tlbpaspeossCsv = getValue("fpasperd.tlbpaspeoss.csv");
        String paspePaspeossGenDistCsv = getValue("fpasperd.paspe.paspeoss.gen.dist.csv");

        logger.info("fpasperd.cicli.ndg.path.csv: " + cicliNdgPathCsv);
        logger.info("fpasperd.tlbpaspe.filter.csv:" + tlbpaspeCsv);
        logger.info("fpasperd.tlbpaspeoss.csv: " + tlbpaspeossCsv);
        logger.info("fpasperd.paspe.paspeoss.gen.dist.csv: " + paspePaspeossGenDistCsv);

        // 19
        Dataset<Row> tlbcidefLoad = readCsvAtPathUsingSchema(cicliNdgPathCsv, FpasperdSchema.getTlbcidefLoadPigSchema());

        // (int)ToString(AddDuration( ToDate( (chararray)datafinedef,'yyyyMMdd' ),'P2M' ),'yyyyMMdd' )	AS  datafinedef
        Column dataFineDefCol = addDurationUDF(toStringCol(tlbcidefLoad.col("datafinedef")), "yyyyMMdd", 2).as("datafinedef");
        Dataset<Row> tlbcidef = tlbcidefLoad
                .select(functions.col("codicebanca"), functions.col("ndgprincipale"),
                        functions.col("datainiziodef"), dataFineDefCol,
                        functions.col("codicebanca_collegato"), functions.col("ndg_collegato"));
        // 56

        // 63
        Dataset<Row> tlbpaspeFilter = readCsvAtPathUsingSchema(tlbpaspeCsv, FpasperdSchema.getTlbpaspeFilterPigSchema());

        // 71

        // 77

        // JOIN tlbpaspe_filter BY (cd_istituto, ndg) LEFT, tlbcidef BY (codicebanca_collegato, ndg_collegato);
        Column tlbcidefTlbPaspeFilterJoinCondition = tlbpaspeFilter.col("cd_istituto").equalTo(tlbcidef.col("codicebanca_collegato"))
                .and(tlbpaspeFilter.col("ndg").equalTo(tlbcidef.col("ndg_collegato")));

        // BY (int)SUBSTRING((chararray)tlbpaspe_filter::datacont,0,6) >= (int)SUBSTRING((chararray)tlbcidef::datainiziodef,0,6)
        Column fpasperdBetweenGenDataContDataInizioDefFilterCol = substring06ToInt(tlbpaspeFilter.col("datacont"))
                .geq(substring06ToInt(tlbcidef.col("datainiziodef")));

        // AND (int)SUBSTRING((chararray)tlbpaspe_filter::datacont,0,6) < (int)SUBSTRING( (chararray)tlbcidef::datafinedef,0,6 )
        Column fpasperdBetweenGenDataContDataFineDefFilterCol = substring06ToInt(tlbpaspeFilter.col("datacont"))
                .lt(substring06ToInt(tlbcidef.col("datafinedef")));

        // DaysBetween( ToDate((chararray)tlbcidef::datafinedef,'yyyyMMdd' ), ToDate((chararray)tlbpaspe_filter::datacont,'yyyyMMdd' ) ) as days_diff
        Column fpasperdBetweenGenDaysDiffColl = daysBetweenUDF(toStringCol(tlbcidef.col("datafinedef")), toStringCol(tlbpaspeFilter.col("datacont")), "yyyyMMdd");

        Dataset<Row> fpasperdBetweenGen = tlbpaspeFilter.join(tlbcidef, tlbcidefTlbPaspeFilterJoinCondition, "left")
                .filter(fpasperdBetweenGenDataContDataInizioDefFilterCol.and(fpasperdBetweenGenDataContDataFineDefFilterCol))
                .select(tlbpaspeFilter.col("cd_istituto"), tlbpaspeFilter.col("ndg"), tlbpaspeFilter.col("datacont"),
                        tlbpaspeFilter.col("causale"), tlbpaspeFilter.col("importo"), tlbcidef.col("codicebanca"),
                        tlbcidef.col("ndgprincipale"), tlbcidef.col("datainiziodef"), tlbcidef.col("datafinedef"))
                .withColumn("days_diff", fpasperdBetweenGenDaysDiffColl);

        // 104

        // 109
        // GROUP fpasperd_between_gen BY ( cd_istituto, ndg, datacont, causale, codicebanca, ndgprincipale );
        // ... ORDER fpasperd_between_gen by days_diff ASC
        WindowSpec fpasperdBetweenOutWindowSpec = Window.partitionBy("cd_istituto", "ndg", "datacont", "causale",
                "codicebanca", "ndgprincipale").orderBy(functions.col("days_diff").asc());

        Dataset<Row> fpasperdBetweenOut = fpasperdBetweenGen
                .select(functions.col("cd_istituto"), functions.col("ndg"),
                        functions.col("datacont"), functions.col("causale"),
                        functions.first("importo").over(fpasperdBetweenOutWindowSpec).as("importo"),
                        functions.col("codicebanca"), functions.col("ndgprincipale"),
                        functions.first("datainiziodef").over(fpasperdBetweenOutWindowSpec).as("datainiziodef"))
                .distinct();
        // 127

        // 132

        Column codiceBancaNullCol = toStringCol(functions.lit(null)).as("codicebanca");
        Column ndgPrincipaleNullCol = toStringCol(functions.lit(null)).as("ndgprincipale");
        Column dataInizioDefNullCol = toStringCol(functions.lit(null)).as("datainiziodef");

        // ... = JOIN tlbpaspe_filter BY (cd_istituto, ndg) LEFT, tlbcidef BY (codicebanca_collegato, ndg_collegato);
        // FILTER ... BY tlbcidef::codicebanca IS NOT NULL
        Dataset<Row> fpasperdOtherGen = tlbpaspeFilter.join(tlbcidef, tlbcidefTlbPaspeFilterJoinCondition, "left_semi")
                .select(tlbpaspeFilter.col("cd_istituto"), tlbpaspeFilter.col("ndg"), tlbpaspeFilter.col("datacont"),
                        tlbpaspeFilter.col("causale"), tlbpaspeFilter.col("importo"), codiceBancaNullCol, ndgPrincipaleNullCol,
                        dataInizioDefNullCol);

        // 147

        // 152

        // ... = JOIN fpasperd_other_gen BY (cd_istituto, ndg, datacont) LEFT, fpasperd_between_out BY (cd_istituto, ndg, datacont);
        // FILTER ... BY fpasperd_between_out::cd_istituto IS NULL

        Dataset<Row> fpasperdOtherOut = fpasperdOtherGen.join(fpasperdBetweenOut,
                toScalaSeq(Arrays.asList("cd_istituto", "ndg", "datacont")),
                "left_anti");

        // 170

        // 175

        // ... = JOIN tlbpaspe_filter BY (cd_istituto, ndg) LEFT, tlbcidef BY (codicebanca_collegato, ndg_collegato);
        // FILTER ... BY tlbcidef::codicebanca IS NULL

        Dataset<Row> fpasperdNullOut = tlbpaspeFilter.join(tlbcidef, tlbcidefTlbPaspeFilterJoinCondition, "left_anti")
                .select(tlbpaspeFilter.col("cd_istituto"), tlbpaspeFilter.col("ndg"), tlbpaspeFilter.col("datacont"),
                        tlbpaspeFilter.col("causale"), tlbpaspeFilter.col("importo"), codiceBancaNullCol, ndgPrincipaleNullCol,
                        dataInizioDefNullCol);

        // 190

        // 198

        // JOIN fpasperd_null_out BY (cd_istituto, ndg) LEFT, tlbcidef BY (codicebanca, ndgprincipale);
        Column principFpasperdBetweenGenJoinCondition = fpasperdNullOut.col("cd_istituto").equalTo(tlbcidef.col("codicebanca"))
                .and(fpasperdNullOut.col("ndg").equalTo(tlbcidef.col("ndgprincipale")));

        //  BY (int)SUBSTRING((chararray)fpasperd_null_out::datacont,0,6) >= (int)SUBSTRING((chararray)tlbcidef::datainiziodef,0,6)
        Column principFpasperdBetweenGenDataContDataInizioDefFilterCol =
                substring06ToInt(fpasperdNullOut.col("datacont"))
                        .geq(substring06ToInt(tlbcidef.col("datainiziodef")));

        // AND (int)SUBSTRING((chararray)fpasperd_null_out::datacont,0,6) < (int)SUBSTRING( (chararray)tlbcidef::datafinedef,0,6 )
        Column principFpasperdBetweenGenDataContDataFineDefFilterCol =
                substring06ToInt(fpasperdNullOut.col("datacont"))
                        .lt(substring06ToInt(tlbcidef.col("datafinedef")));

        // DaysBetween( ToDate((chararray)tlbcidef::datafinedef,'yyyyMMdd' ),
        // ToDate((chararray)fpasperd_null_out::datacont,'yyyyMMdd' ) ) as days_diff
        Column principFpasperdBetweenGenDaysDiffColl = daysBetweenUDF(
                toStringCol(tlbcidef.col("datafinedef")),
                toStringCol(fpasperdNullOut.col("datacont")), "yyyyMMdd");

        Dataset<Row> principFpasperdBetweenGen = fpasperdNullOut.join(tlbcidef, principFpasperdBetweenGenJoinCondition, "left")
                .filter(principFpasperdBetweenGenDataContDataInizioDefFilterCol.and(principFpasperdBetweenGenDataContDataFineDefFilterCol))
                .select(fpasperdNullOut.col("cd_istituto"), fpasperdNullOut.col("ndg"), fpasperdNullOut.col("datacont"),
                        fpasperdNullOut.col("causale"), fpasperdNullOut.col("importo"), tlbcidef.col("codicebanca"),
                        tlbcidef.col("ndgprincipale"), tlbcidef.col("datainiziodef"), tlbcidef.col("datafinedef"))
                .withColumn("days_diff", principFpasperdBetweenGenDaysDiffColl);

        // 222

        // 227
        Dataset<Row> principFpasperdBetweenOut = principFpasperdBetweenGen
                .select(functions.col("cd_istituto"), functions.col("ndg"),
                        functions.col("datacont"), functions.col("causale"),
                        functions.first("importo").over(fpasperdBetweenOutWindowSpec).as("importo"),
                        functions.col("codicebanca"), functions.col("ndgprincipale"),
                        functions.first("datainiziodef").over(fpasperdBetweenOutWindowSpec).as("datainiziodef"))
                .distinct();

        // 245

        // 250


        // ... = JOIN fpasperd_null_out BY (cd_istituto, ndg) LEFT, tlbcidef BY (codicebanca, ndgprincipale);
        // FILTER ... BY tlbcidef::codicebanca IS NOT NULL

        Column principFpasperdOtherGenJoinCondition =
                fpasperdNullOut.col("cd_istituto").equalTo(tlbcidef.col("codicebanca"))
                        .and(fpasperdNullOut.col("ndg").equalTo(tlbcidef.col("ndgprincipale")));

        Dataset<Row> principFpasperdOtherGen = fpasperdNullOut.join(tlbcidef, principFpasperdOtherGenJoinCondition, "left_semi")
                .select(fpasperdNullOut.col("cd_istituto"), fpasperdNullOut.col("ndg"),
                        fpasperdNullOut.col("datacont"), fpasperdNullOut.col("causale"),
                        fpasperdNullOut.col("importo"), codiceBancaNullCol, ndgPrincipaleNullCol, dataInizioDefNullCol);

        // 265

        // 270

        // JOIN princip_fpasperd_other_gen BY (cd_istituto, ndg, datacont) LEFT, princip_fpasperd_between_out BY (cd_istituto, ndg, datacont);
        // FILTER BY princip_fpasperd_between_out::cd_istituto IS NULL

        Dataset<Row> principFpasperdOtherOut = principFpasperdOtherGen.join(principFpasperdBetweenOut,
                toScalaSeq(Arrays.asList("cd_istituto", "ndg", "datacont")),
                "left_anti");

        // 288

        // 293

        // ... = JOIN fpasperd_null_out BY (cd_istituto, ndg) LEFT, tlbcidef BY (codicebanca, ndgprincipale);
        // FILTER ... BY tlbcidef::codicebanca IS NULL

        Column principFpasperdNullOutJoinCondition = fpasperdNullOut.col("cd_istituto").equalTo(tlbcidef.col("codicebanca"))
                .and(fpasperdNullOut.col("ndg").equalTo(tlbcidef.col("ndgprincipale")));

        Dataset<Row> principFpasperdNullOut = fpasperdNullOut.join(tlbcidef, principFpasperdNullOutJoinCondition, "left_anti")
                .select(fpasperdNullOut.col("cd_istituto"), fpasperdNullOut.col("ndg"),
                        fpasperdNullOut.col("datacont"), fpasperdNullOut.col("causale"),
                        fpasperdNullOut.col("importo"), codiceBancaNullCol, ndgPrincipaleNullCol, dataInizioDefNullCol);

        // 308

        // 313

        logger.info("Trying to perform union of many DataFrames");

        logNumberAndNameOfColumns(fpasperdBetweenOut, "fpasperdBetweenOut");
        logNumberAndNameOfColumns(fpasperdOtherOut, "fpasperdOtherOut");
        logNumberAndNameOfColumns(principFpasperdBetweenOut, "principFpasperdBetweenOut");
        logNumberAndNameOfColumns(principFpasperdOtherOut, "principFpasperdOtherOut");
        logNumberAndNameOfColumns(principFpasperdNullOut, "principFpasperdNullOut");

        Dataset<Row> fpasperdOutDistinct = fpasperdBetweenOut
                .union(fpasperdOtherOut)
                .union(principFpasperdBetweenOut)
                .union(principFpasperdOtherOut)
                .union(principFpasperdNullOut)
                .distinct();

        logger.info("Successfully performed DataFrames union");

        // 331

        // 336
        Dataset<Row> tlbpaspeoss = readCsvAtPathUsingSchema(tlbpaspeossCsv, FpasperdSchema.getTlbpaspeossPigSchema());
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
                functions.when(tlbpaspeoss.col("_cd_istituto").isNotNull(), tlbpaspeoss.col("_ndg"))
                        .otherwise(fpasperdOutDistinct.col("ndg")))
                .otherwise(tlbpaspeoss.col("_ndg")).as("ndg");

        // ( fpasperd_out_distinct::cd_istituto is not null?
        //      ( tlbpaspeoss::cd_istituto is not null? tlbpaspeoss::datacont : fpasperd_out_distinct::datacont ) :
        //      tlbpaspeoss::datacont ) as datacont

        Column dataContCol = functions.when(fpasperdOutDistinct.col("cd_istituto").isNotNull(),
                functions.when(tlbpaspeoss.col("_cd_istituto").isNotNull(), tlbpaspeoss.col("_datacont"))
                        .otherwise(fpasperdOutDistinct.col("datacont")))
                .otherwise(tlbpaspeoss.col("_datacont")).as("datacont");

        // ( fpasperd_out_distinct::cd_istituto is not null?
        //      ( tlbpaspeoss::cd_istituto is not null? tlbpaspeoss::causale : fpasperd_out_distinct::causale ) :
        //      tlbpaspeoss::causale ) as causale

        Column causaleCol = functions.when(fpasperdOutDistinct.col("cd_istituto").isNotNull(),
                functions.when(tlbpaspeoss.col("_cd_istituto").isNotNull(), tlbpaspeoss.col("_causale"))
                        .otherwise(fpasperdOutDistinct.col("causale")))
                .otherwise(tlbpaspeoss.col("_causale")).as("causale");

        // ( fpasperd_out_distinct::cd_istituto is not null?
        //      ( tlbpaspeoss::cd_istituto is not null? tlbpaspeoss::importo : fpasperd_out_distinct::importo ) :
        //      tlbpaspeoss::importo ) as importo

        Column importoCol = functions.when(fpasperdOutDistinct.col("cd_istituto").isNotNull(),
                functions.when(tlbpaspeoss.col("_cd_istituto").isNotNull(), tlbpaspeoss.col("_importo"))
                        .otherwise(fpasperdOutDistinct.col("importo")))
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

        writeDatasetAsCsvAtPath(paspePaspeossGenDist, paspePaspeossGenDistCsv);
    }

    private void logNumberAndNameOfColumns(Dataset<Row> dataset, String datasetName) {

        int numberOfColumns = dataset.columns().length;
        String columnNames = String.join(", ", dataset.columns());
        logger.info(String.format("DataFrame %s has %s columns (%s)", datasetName, numberOfColumns, columnNames));
    }

    private Column substring06ToInt(Column column) {

        return substringAndToInt(column, 0, 6);
    }
}
