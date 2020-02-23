package steps.lgdstep;

import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import steps.abstractstep.AbstractStep;
import steps.schemas.QuadFposiSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static steps.abstractstep.StepUtils.*;

public class QuadFposi extends AbstractStep {

    private final Logger logger = Logger.getLogger(QuadFposi.class);

    // required parameters
    private String ufficio;

    public QuadFposi(String ufficio){

        this.ufficio = ufficio;
        stepInputDir = getValue("quad.fposi.input.dir");
        stepOutputDir = getValue("quad.fposi.output.dir");

        logger.debug("ufficio: " + this.ufficio);
        logger.debug("stepInputDir: " + stepInputDir);
        logger.debug("stepOutputDir: " + stepOutputDir);

    }

    @Override
    public void run() {

        String hadoopFposiCsv = getValue("quad.fposi.hadoop.fposi.csv");
        String oldfposiLoadCsv = getValue("quad.fposi.old.fposi.load.csv");
        String hadoopFposiOutDir = getValue("quad.fposi.hadoop.fposi.out");
        String oldFposiOutDir = getValue("quad.fposi.old.fposi.out");
        String abbinatiOutDir = getValue("quad.fposi.abbinati.out");

        logger.debug("hadoopFposiCsv: " + hadoopFposiCsv);
        logger.debug("oldfposiLoadCsv: " + oldfposiLoadCsv);
        logger.debug("hadoopFposiOutDir: " + hadoopFposiOutDir);
        logger.debug("oldFposiOutDir: " + oldFposiOutDir);
        logger.debug("abbinatiOutDir: " + abbinatiOutDir);

        // 17
        Dataset<Row> hadoopFposi = readCsvAtPathUsingSchema(hadoopFposiCsv,
                fromPigSchemaToStructType(QuadFposiSchema.getHadoopFposiPigSchema()));

        // 51
        Dataset<Row> oldfposiLoad = readCsvAtPathUsingSchema(oldfposiLoadCsv,
                fromPigSchemaToStructType(QuadFposiSchema.getOldFposiLoadPigSchema()));

        // 70

        /*
        ToString(ToDate( datainizioDEF,'yy-MM-dd'),'yyyyMMdd')   as DATAINIZIODEF
        ,ToString(ToDate( dataFINEDEF,'yy-MM-dd'),'yyyyMMdd')     as DATAFINEDEF
        ,ToString(ToDate( dataINIZIOPD,'yy-MM-dd'),'yyyyMMdd')    as DATAINIZIOPD
        ,ToString(ToDate( datainizioinc,'yy-MM-dd'),'yyyyMMdd')   as DATAINIZIOINC
        ,ToString(ToDate( dataSOFFERENZA,'yy-MM-dd'),'yyyyMMdd')  as DATASOFFERENZA
         */

        String oldDatePattern = "yy-MM-dd";
        String newDatePattern = "yyyyMMdd";
        Column DATAINIZIODEFCol = changeDateFormat(oldfposiLoad.col("datainizioDEF"), oldDatePattern, newDatePattern).alias("DATAINIZIODEF");
        Column DATAFINEDEFCol = changeDateFormat(oldfposiLoad.col("dataFINEDEF"), oldDatePattern, newDatePattern).alias("DATAFINEDEF");
        Column DATAINIZIOPDCol = changeDateFormat(oldfposiLoad.col("dataINIZIOPD"), oldDatePattern, newDatePattern).alias("DATAINIZIOPD");
        Column DATAINIZIOINCCol = changeDateFormat(oldfposiLoad.col("datainizioinc"), oldDatePattern, newDatePattern).alias("DATAINIZIOINC");
        Column DATASOFFERENZACol = changeDateFormat(oldfposiLoad.col("dataSOFFERENZA"), oldDatePattern, newDatePattern).alias("DATASOFFERENZA");

        Dataset<Row> oldFposiGen = oldfposiLoad.select(DATAINIZIODEFCol, DATAFINEDEFCol, DATAINIZIOPDCol, DATAINIZIOINCCol, DATASOFFERENZACol,
                oldfposiLoad.col("codicebanca").alias("CODICEBANCA"), oldfposiLoad.col("ndgprincipale").alias("NDGPRINCIPALE"),
                oldfposiLoad.col("flagincristrut").alias("FLAGINCRISTRUT"), oldfposiLoad.col("cumulo").alias("CUMULO"));

        // 81

        // 83

        /*
        FILTER oldfposi_gen
        BY ToDate( DATAINIZIODEF,'yyyyMMdd') >= ToDate( '20070131','yyyyMMdd' )
        and ToDate( DATAINIZIODEF,'yyyyMMdd') <= ToDate( '20071231','yyyyMMdd' );
         */

        Column DATAINIZIODEFFilterCol = isDateBetween(oldFposiGen.col("DATAINIZIODEF"), newDatePattern,
                "20070131", "yyyyMMdd", "20071231", "yyyyMMdd");
        Dataset<Row> oldFposi = oldFposiGen.filter(DATAINIZIODEFFilterCol);

        // 85

        // JOIN hadoop_fposi BY (codicebanca, ndgprincipale, datainiziodef) FULL OUTER, oldfposi BY (CODICEBANCA, NDGPRINCIPALE, DATAINIZIODEF);
        Column joinCondition = hadoopFposi.col("codicebanca").equalTo(oldFposi.col("CODICEBANCA"))
                .and(hadoopFposi.col("ndgprincipale").equalTo(oldFposi.col("NDGPRINCIPALE")))
                .and(hadoopFposi.col("datainiziodef").equalTo(oldFposi.col("DATAINIZIODEF")));

        Dataset<Row> hadoopFposiOldFposiJoin = hadoopFposi.join(oldFposi, joinCondition, "full_outer");

        List<Column> selectColList = new ArrayList<>(Collections.singletonList(functions.lit(ufficio).alias("ufficio")));
        List<Column> hadoopFposiSelectList = selectDfColumns(hadoopFposi, Arrays.asList(hadoopFposi.columns()));
        List<Column> oldFposiSelectList = selectDfColumns(oldFposi, Arrays.asList(oldFposi.columns()));

        selectColList.addAll(hadoopFposiSelectList);
        selectColList.addAll(oldFposiSelectList);

        Dataset<Row> hadoopFposiOut = hadoopFposiOldFposiJoin.filter(oldFposi.col("CODICEBANCA").isNull()).select(toScalaColSeq(selectColList));
        Dataset<Row> oldFposiOut = hadoopFposiOldFposiJoin.filter(hadoopFposi.col("codicebanca").isNull()).select(toScalaColSeq(selectColList));

        /*
        FILTER hadoop_fposi_oldfposi_join
        BY hadoop_fposi::codicebanca IS NOT NULL
        AND oldfposi::CODICEBANCA IS NOT NULL
        AND hadoop_fposi::datafinedef == '99991231'
        AND hadoop_fposi::datainiziopd       != oldfposi::DATAINIZIOPD
        AND hadoop_fposi::datainizioinc      != oldfposi::DATAINIZIOINC
        AND hadoop_fposi::datainiziosoff     != oldfposi::DATASOFFERENZA
         */

        Column abbinatiOutFilterCol = hadoopFposi.col("codicebanca").isNotNull()
                .and(oldFposi.col("CODICEBANCA").isNotNull())
                .and(hadoopFposi.col("datafinedef").equalTo("99991231"))
                .and(hadoopFposi.col("datainiziopd").notEqual(oldFposi.col("DATAINIZIOPD")))
                .and(hadoopFposi.col("datainizioinc").notEqual(oldFposi.col("DATAINIZIOINC")))
                .and(hadoopFposi.col("datainiziosoff").notEqual(oldFposi.col("DATASOFFERENZA")));

        Dataset<Row> abbinatiOut = hadoopFposiOldFposiJoin.filter(abbinatiOutFilterCol).select(toScalaColSeq(selectColList));

        writeDatasetAsCsvAtPath(hadoopFposiOut, hadoopFposiOutDir);
        writeDatasetAsCsvAtPath(oldFposiOut, oldFposiOutDir);
        writeDatasetAsCsvAtPath(abbinatiOut, abbinatiOutDir);
    }
}
