package it.carloni.luca.lgd.steps;

import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import scala.collection.Seq;
import it.carloni.luca.lgd.commons.AbstractStep;
import it.carloni.luca.lgd.commons.StepUtils;
import it.carloni.luca.lgd.schemas.FrappPumaSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static it.carloni.luca.lgd.commons.StepUtils.*;

public class FrappPuma extends AbstractStep {

    private final Logger logger = Logger.getLogger(FrappPuma.class);

    // required parameters
    private String dataA;

    public FrappPuma(String dataA){

        this.dataA = dataA;
        stepInputDir = getValue("frapp.puma.input.dir");
        stepOutputDir = getValue("frapp.puma.output.dir");

        logger.debug("stepInputDir: " + stepInputDir);
        logger.debug("stepOutputDir: " + stepOutputDir);
        logger.debug("dataA: " + this.dataA);
    }

    @Override
    public void run() {

        String cicliNdgPath = getValue("frapp.puma.cicli.ndg.path");
        String tlbgaranPath = getValue("frapp.puma.tlbgaran.path");
        String frappPumaOutPath = getValue("frapp.puma.frapp.puma.out");

        logger.debug("cicliNdgPath: " + cicliNdgPath);
        logger.debug("tlbgaranPath:" + tlbgaranPath);
        logger.debug("frappPumaOutPath: " + frappPumaOutPath);

        // 22

        Dataset<Row> tlbcidef = readCsvAtPathUsingSchema(cicliNdgPath,
                fromPigSchemaToStructType(FrappPumaSchema.getTlbcidefPigSchema()));
        // 49

        // cicli_ndg_princ = FILTER tlbcidef BY cd_collegamento IS NULL;
        // cicli_ndg_coll = FILTER tlbcidef BY cd_collegamento IS NOT NULL;
        Dataset<Row> cicliNdgPrinc = tlbcidef.filter(tlbcidef.col("cd_collegamento").isNull());
        Dataset<Row> cicliNdgColl = tlbcidef.filter(tlbcidef.col("cd_collegamento").isNotNull());

        // 59
        Dataset<Row> tlbgaran = readCsvAtPathUsingSchema(tlbgaranPath,
                fromPigSchemaToStructType(FrappPumaSchema.getTlbgaranPigSchema()));

        // 71

        // JOIN  tlbgaran BY (cd_istituto, ndg), cicli_ndg_princ BY (codicebanca_collegato, ndg_collegato);
        Column tlbcidefTlbgaranPrincJoinCondition = tlbgaran.col("cd_istituto").equalTo(cicliNdgPrinc.col("codicebanca_collegato"))
                .and(tlbgaran.col("ndg").equalTo(cicliNdgPrinc.col("ndg_collegato")));

        // ToDate( (chararray)dt_riferimento,'yyyyMMdd') >= ToDate( (chararray)datainiziodef,'yyyyMMdd' )
        Column dtRiferimentoDataInizioDefFilterCol = tlbgaran.col("dt_riferimento").geq(tlbcidef.col("datainiziodef"));

        /* and SUBSTRING( (chararray)dt_riferimento,0,6 ) <=
            SUBSTRING( (chararray)LeastDate( (int)ToString(SubtractDuration(ToDate((chararray)datafinedef,'yyyyMMdd' ),'P1M'),'yyyyMMdd') ,
                        $data_a), 0,6 );
        */

        // we need to format $data_a to yyyyMMdd
        String dataAPattern = getValue("params.dataa.pattern");
        Column dataACol = functions.lit(changeDateFormat(dataA, dataAPattern, "yyyyMMdd"));
        Column dataFineDefDataALeastDateCol = leastDate(subtractDuration(StepUtils.toStringCol(tlbcidef.col("datafinedef")), "yyyyMMdd", 1),
                dataACol, "yyyMMdd");

        Column dtRiferimentoLeastDateFilterCol = substringAndCastToInt(StepUtils.toStringCol(tlbgaran.col("dt_riferimento")), 0, 6)
                .leq(substringAndCastToInt(StepUtils.toStringCol(dataFineDefDataALeastDateCol), 0, 6));

        /*
          tlbgaran::cd_istituto			 AS cd_isti
         ,tlbgaran::ndg					 AS ndg
         ,tlbgaran::sportello			 AS sportello
         ,tlbgaran::dt_riferimento		 AS dt_riferimento
         ,tlbgaran::conto_esteso		 AS conto_esteso
         ,tlbgaran::cd_puma2			 AS cd_puma2
         ,tlbgaran::ide_garanzia		 AS ide_garanzia
         ,tlbgaran::importo				 AS importo
         ,tlbgaran::fair_value			 AS fair_value
         ,cicli_ndg_princ::codicebanca	 AS codicebanca
         ,cicli_ndg_princ::ndgprincipale AS ndgprincipale
         ,cicli_ndg_princ::datainiziodef AS	datainiziodef
         */
        List<Column> tlbcidefTlbgaranPrincSelectColsList = new ArrayList<>(
                Collections.singletonList(tlbgaran.col("cd_istituto").alias("cd_isti")));

        List<String> tlbgaranSelectColNames = Arrays.asList(
                "ndg", "sportello", "dt_riferimento", "conto_esteso",
                "cd_puma2", "ide_garanzia", "importo", "fair_value");
        List<String> cicliNdgSelectColNames = Arrays.asList("codicebanca", "ndgprincipale", "datainiziodef");
        tlbcidefTlbgaranPrincSelectColsList.addAll(selectDfColumns(tlbgaran, tlbgaranSelectColNames));
        tlbcidefTlbgaranPrincSelectColsList.addAll(selectDfColumns(cicliNdgPrinc, cicliNdgSelectColNames));

        Seq<Column> tlbcidefTlbgaranPrincSelectColsSeq = toScalaColSeq(tlbcidefTlbgaranPrincSelectColsList);

        Dataset<Row> tlbcidefTlbgaranPrinc = tlbgaran.join(cicliNdgPrinc, tlbcidefTlbgaranPrincJoinCondition, "inner")
                .filter(dtRiferimentoDataInizioDefFilterCol.and(dtRiferimentoLeastDateFilterCol))
                .select(tlbcidefTlbgaranPrincSelectColsSeq);

        // JOIN  tlbgaran BY (cd_istituto, ndg, dt_riferimento), cicli_ndg_coll BY (codicebanca_collegato, ndg_collegato, dt_rif_udct);
        Column tlbcidefTlbgaranCollJoinCondition = tlbgaran.col("cd_istituto").equalTo(cicliNdgColl.col("codicebanca_collegato"))
                .and(tlbgaran.col("ndg").equalTo(cicliNdgColl.col("ndg_collegato")))
                .and(tlbgaran.col("dt_riferimento").equalTo(cicliNdgColl.col("dt_rif_udct")));

        List<Column> tlbcidefTlbgaranCollSelectColsList = new ArrayList<>(
                Collections.singletonList(tlbgaran.col("cd_istituto").alias("cd_isti")));
        tlbcidefTlbgaranCollSelectColsList.addAll(selectDfColumns(tlbgaran, tlbgaranSelectColNames));
        tlbcidefTlbgaranCollSelectColsList.addAll(selectDfColumns(cicliNdgColl, cicliNdgSelectColNames));
        Seq<Column> tlbcidefTlbgaranCollSelectColsSeq = toScalaColSeq(tlbcidefTlbgaranCollSelectColsList);

        Dataset<Row> tlbcidefTlbgaranColl = tlbgaran.join(cicliNdgColl, tlbcidefTlbgaranCollJoinCondition, "inner")
                .select(tlbcidefTlbgaranCollSelectColsSeq);

        Dataset<Row> frappPumaOut = tlbcidefTlbgaranPrinc.union(tlbcidefTlbgaranColl).distinct();
        writeDatasetAsCsvAtPath(frappPumaOut, frappPumaOutPath);
    }
}
