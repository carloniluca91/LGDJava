package it.carloni.luca.lgd.spark.step;

import it.carloni.luca.lgd.parameter.step.DataAValue;
import it.carloni.luca.lgd.schema.FrappPumaSchema;
import it.carloni.luca.lgd.spark.common.AbstractStep;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import static it.carloni.luca.lgd.spark.utils.StepUtils.*;

public class FrappPuma extends AbstractStep<DataAValue> {

    private final Logger logger = Logger.getLogger(getClass());

    public void run(DataAValue dataAValue) {

        String dataA = dataAValue.getDataA();

        logger.info(dataAValue.toString());

        String cicliNdgPath = getValue("frapp.puma.cicli.ndg.path");
        String tlbgaranPath = getValue("frapp.puma.tlbgaran.path");
        String frappPumaOutPath = getValue("frapp.puma.frapp.puma.out");

        logger.info("frapp.puma.cicli.ndg.path: " + cicliNdgPath);
        logger.info("frapp.puma.tlbgaran.path:" + tlbgaranPath);
        logger.info("frapp.puma.frapp.puma.out: " + frappPumaOutPath);

        // 22

        Dataset<Row> tlbcidef = readCsvAtPathUsingSchema(cicliNdgPath, FrappPumaSchema.getTlbcidefPigSchema());
        // 49

        // cicli_ndg_princ = FILTER tlbcidef BY cd_collegamento IS NULL;
        // cicli_ndg_coll = FILTER tlbcidef BY cd_collegamento IS NOT NULL;

        Dataset<Row> cicliNdgPrinc = tlbcidef.filter(tlbcidef.col("cd_collegamento").isNull());
        Dataset<Row> cicliNdgColl = tlbcidef.filter(tlbcidef.col("cd_collegamento").isNotNull());

        // 59
        Dataset<Row> tlbgaran = readCsvAtPathUsingSchema(tlbgaranPath, FrappPumaSchema.getTlbgaranPigSchema());

        // 71

        // JOIN  tlbgaran BY (cd_istituto, ndg), cicli_ndg_princ BY (codicebanca_collegato, ndg_collegato);
        Column tlbcidefTlbgaranPrincJoinCondition = tlbgaran.col("cd_istituto").equalTo(cicliNdgPrinc.col("codicebanca_collegato"))
                .and(tlbgaran.col("ndg").equalTo(cicliNdgPrinc.col("ndg_collegato")));

        // ToDate( (chararray)dt_riferimento,'yyyyMMdd') >= ToDate( (chararray)datainiziodef,'yyyyMMdd' )
        Column dtRiferimentoDataInizioDefFilterCol = tlbgaran.col("dt_riferimento").geq(cicliNdgPrinc.col("datainiziodef"));

        /* and SUBSTRING( (chararray)dt_riferimento,0,6 ) <=
            SUBSTRING( (chararray)LeastDate( (int)ToString(SubtractDuration(ToDate((chararray)datafinedef,'yyyyMMdd' ),'P1M'),'yyyyMMdd') ,
                        $data_a), 0,6 );
        */

        // we need to format $data_a to yyyyMMdd
        String dataAPattern = getValue("params.dataa.pattern");
        Column dataACol = functions.lit(changeDateFormat(dataA, dataAPattern, "yyyyMMdd"));
        Column dataFineDefDataALeastDateCol = leastDateUDF(
                subtractDurationUDF(toStringCol(cicliNdgPrinc.col("datafinedef")), "yyyyMMdd", 1),
                dataACol,
                "yyyMMdd");

        Column dtRiferimentoLeastDateFilterCol = substring06(tlbgaran.col("dt_riferimento")).leq(substring06(dataFineDefDataALeastDateCol));

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

        Dataset<Row> tlbcidefTlbgaranPrinc = tlbgaran.join(cicliNdgPrinc, tlbcidefTlbgaranPrincJoinCondition)
                .filter(dtRiferimentoDataInizioDefFilterCol.and(dtRiferimentoLeastDateFilterCol))
                .select(tlbgaran.col("cd_istituto").alias("cd_isti"), tlbgaran.col("ndg"), tlbgaran.col("sportello"),
                        tlbgaran.col("dt_riferimento"), tlbgaran.col("conto_esteso"), tlbgaran.col("cd_puma2"),
                        tlbgaran.col("ide_garanzia"), tlbgaran.col("importo"), tlbgaran.col("fair_value"),
                        cicliNdgPrinc.col("codicebanca"), cicliNdgPrinc.col("ndgprincipale"),
                        cicliNdgPrinc.col("datainiziodef"));

        // JOIN  tlbgaran BY (cd_istituto, ndg, dt_riferimento), cicli_ndg_coll BY (codicebanca_collegato, ndg_collegato, dt_rif_udct);
        Column tlbcidefTlbgaranCollJoinCondition = tlbgaran.col("cd_istituto").equalTo(cicliNdgColl.col("codicebanca_collegato"))
                .and(tlbgaran.col("ndg").equalTo(cicliNdgColl.col("ndg_collegato")))
                .and(tlbgaran.col("dt_riferimento").equalTo(cicliNdgColl.col("dt_rif_udct")));

        Dataset<Row> tlbcidefTlbgaranColl = tlbgaran.join(cicliNdgColl, tlbcidefTlbgaranCollJoinCondition)
                .select(tlbgaran.col("cd_istituto").alias("cd_isti"), tlbgaran.col("ndg"), tlbgaran.col("sportello"),
                        tlbgaran.col("dt_riferimento"), tlbgaran.col("conto_esteso"), tlbgaran.col("cd_puma2"),
                        tlbgaran.col("ide_garanzia"), tlbgaran.col("importo"), tlbgaran.col("fair_value"),
                        cicliNdgColl.col("codicebanca"), cicliNdgColl.col("ndgprincipale"),
                        cicliNdgColl.col("datainiziodef"));

        Dataset<Row> frappPumaOut = tlbcidefTlbgaranPrinc
                .union(tlbcidefTlbgaranColl)
                .distinct();

        writeDatasetAsCsvAtPath(frappPumaOut, frappPumaOutPath);
    }

    private Column substring06(Column column) {

        return functions.substring(column.cast(DataTypes.StringType), 0, 6);
    }
}
