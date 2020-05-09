package it.carloni.luca.lgd.spark.step;

import it.carloni.luca.lgd.parameter.step.DataANumeroMesi12Values;
import it.carloni.luca.lgd.spark.common.AbstractStep;
import it.carloni.luca.lgd.spark.utils.StepUtils;
import it.carloni.luca.lgd.schema.FrappNdgMonthlySchema;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import static it.carloni.luca.lgd.spark.utils.StepUtils.*;

public class FrappNdgMonthly extends AbstractStep<DataANumeroMesi12Values> {

    private final Logger logger = Logger.getLogger(FrappNdgMonthly.class);

    @Override
    public void run(DataANumeroMesi12Values stepValues) {

        String dataA = stepValues.getDataA();
        Integer numeroMesi1 = stepValues.getNumeroMesi1();
        Integer numeroMesi2 = stepValues.getNumeroMesi2();

        logger.info(stepValues.toString());

        String cicliNdgPathCsvPath = getValue("frapp.ndg.monthly.cicli.ndg.path.csv");
        String tlburttCsvPath = getValue("frapp.ndg.monthly.tlburtt.csv");
        String tlbcidefTlburttCsv = getValue("frapp.ndg.monthly.tlbcidef.tlburtt");

        logger.info("frapp.ndg.monthly.cicli.ndg.path.csv: " + cicliNdgPathCsvPath);
        logger.info("frapp.ndg.monthly.tlburtt.csv: " + tlburttCsvPath);
        logger.info("frapp.ndg.monthly.tlbcidef.tlburtt: " + tlbcidefTlburttCsv);

        // 26
        Dataset<Row> tlbcidef = readCsvAtPathUsingSchema(cicliNdgPathCsvPath, FrappNdgMonthlySchema.getTlbcidefPigSchema());
        // 53

        // 58
        Dataset<Row> cicliNdgPrinc = tlbcidef.filter(tlbcidef.col("cd_collegamento").isNull());
        Dataset<Row> cicliNdgColl = tlbcidef.filter(tlbcidef.col("cd_collegamento").isNotNull());
        // 60

        // 69
        Dataset<Row> tlburtt = readCsvAtPathUsingSchema(tlburttCsvPath, FrappNdgMonthlySchema.getTlburttPigSchema());
        // 107

        // 111
        Dataset<Row> tlburttFilter = tlburtt.filter(tlburtt.col("progr_segmento").equalTo(0));

        // ToDate((chararray)dt_riferimento,'yyyyMMdd') >= SubtractDuration(ToDate((chararray)datainiziodef,'yyyyMMdd'),'$numero_mesi_1')
        Column dtRiferimentoFilterPrincCol = tlburttFilter.col("dt_riferimento").geq(
                toIntCol(subtractDurationUDF(StepUtils.toStringCol(cicliNdgPrinc.col("datainiziodef")), "yyyyMMdd", numeroMesi1)));

        /*
        AddDuration(ToDate((chararray)
            LeastDate((int)ToString(
                SubtractDuration(ToDate((chararray)datafinedef,'yyyyMMdd' ),'P1M'),'yyyyMMdd'),
                $data_a),
            'yyyyMMdd' ),'$numero_mesi_2' )
;
         */

        // we need to format $data_a to yyyyMMdd
        String dataAPattern = getValue("params.dataa.pattern");
        Column dataACol = functions.lit(changeDateFormat(dataA, dataAPattern, "yyyyMMdd"));
        Column dataFineDefSubtractDurationPrincCol = subtractDurationUDF(StepUtils.toStringCol(cicliNdgPrinc.col("datafinedef")), "yyyyMMdd", 1);
        Column leastDateDataFineDefDataAPrincCol = leastDateUDF(dataFineDefSubtractDurationPrincCol, dataACol, "yyyyMMdd");
        Column addDurationLeastDateDataFineDefDataAPrincCol = addDurationUDF(leastDateDataFineDefDataAPrincCol, "yyyyMMdd", numeroMesi2);

        // AND SUBSTRING( (chararray)dt_riferimento,0,6 ) <= SUBSTRING(AddDuration(...), 0, 6)
        Column dataFineDefFilterPrincCol = substringAndToInt(StepUtils.toStringCol(tlburttFilter.col("dt_riferimento")), 0, 6)
                .leq(substringAndToInt(addDurationLeastDateDataFineDefDataAPrincCol, 0, 6));

        Dataset<Row> tlbcidefUrttPrinc = cicliNdgPrinc.join(tlburttFilter, cicliNdgPrinc.col("codicebanca_collegato").equalTo(
                tlburttFilter.col("cd_istituto")).and(cicliNdgPrinc.col("ndg_collegato").equalTo(tlburttFilter.col("ndg"))))
                .filter(dtRiferimentoFilterPrincCol.and(dataFineDefFilterPrincCol))
                .select(cicliNdgPrinc.col("codicebanca"), cicliNdgPrinc.col("ndgprincipale"),
                        cicliNdgPrinc.col("codicebanca_collegato"), cicliNdgPrinc.col("ndg_collegato"),
                        cicliNdgPrinc.col("datainiziodef"), cicliNdgPrinc.col("datafinedef"),
                        tlburttFilter.col("cd_istituto"), tlburttFilter.col("ndg"), tlburttFilter.col("sportello"),
                        tlburttFilter.col("conto"), tlburttFilter.col("dt_riferimento"), tlburttFilter.col("conto_esteso"),
                        tlburttFilter.col("forma_tecnica"), tlburttFilter.col("dt_accensione"),
                        tlburttFilter.col("dt_estinzione"), tlburttFilter.col("dt_scadenza"),
                        tlburttFilter.col("tp_ammortamento"), tlburttFilter.col("tp_rapporto"),
                        tlburttFilter.col("period_liquid"), tlburttFilter.col("cd_prodotto_ris"),
                        tlburttFilter.col("durata_originaria"), tlburttFilter.col("divisa"),
                        tlburttFilter.col("durata_residua"), tlburttFilter.col("tp_contr_rapp"));
        // 158

        // ToDate((chararray)dt_riferimento,'yyyyMMdd') >= SubtractDuration(ToDate((chararray)datainiziodef,'yyyyMMdd'),'$numero_mesi_1')
        Column dtRiferimentoFilterCollCol = tlburttFilter.col("dt_riferimento")
                .geq(subtractDurationUDF(StepUtils.toStringCol(cicliNdgColl.col("datainiziodef")), "yyyyMMdd", numeroMesi1));

        /*
        AddDuration(ToDate((chararray)
            LeastDate((int)ToString(
                SubtractDuration(ToDate((chararray)datafinedef,'yyyyMMdd' ),'P1M'),'yyyyMMdd'),
                $data_a),
            'yyyyMMdd' ),'$numero_mesi_2' )
;
         */

        Column dataFineDefSubtractDurationCollCol = subtractDurationUDF(StepUtils.toStringCol(cicliNdgColl.col("datafinedef")), "yyyyMMdd", 1);
        Column leastDateDataFineDefDataACollCol = leastDateUDF(dataFineDefSubtractDurationCollCol, dataACol, "yyyyMMdd");
        Column addDurationLeastDateDataFineDefDataACollCol = addDurationUDF(leastDateDataFineDefDataACollCol, "yyyyMMdd", numeroMesi2);

        // AND SUBSTRING( (chararray)dt_riferimento,0,6 ) <= SUBSTRING(AddDuration(...), 0, 6)
        Column dataFineDefFilterCollCol = substringAndToInt(StepUtils.toStringCol(tlburttFilter.col("dt_riferimento")), 0, 6)
                .leq(substringAndToInt(StepUtils.toStringCol(addDurationLeastDateDataFineDefDataACollCol), 0, 6));

        Dataset<Row> tlbcidefUrttColl = cicliNdgColl.join(tlburttFilter,
                cicliNdgColl.col("codicebanca_collegato").equalTo(tlburttFilter.col("cd_istituto"))
                        .and(cicliNdgColl.col("ndg_collegato").equalTo(tlburttFilter.col("ndg"))))
                .filter(dtRiferimentoFilterCollCol.and(dataFineDefFilterCollCol))
                .select(cicliNdgColl.col("codicebanca"), cicliNdgColl.col("ndgprincipale"),
                        cicliNdgColl.col("codicebanca_collegato"), cicliNdgColl.col("ndg_collegato"),
                        cicliNdgColl.col("datainiziodef"), cicliNdgColl.col("datafinedef"),
                        tlburttFilter.col("cd_istituto"), tlburttFilter.col("ndg"), tlburttFilter.col("sportello"),
                        tlburttFilter.col("conto"), tlburttFilter.col("dt_riferimento"), tlburttFilter.col("conto_esteso"),
                        tlburttFilter.col("forma_tecnica"), tlburttFilter.col("dt_accensione"),
                        tlburttFilter.col("dt_estinzione"), tlburttFilter.col("dt_scadenza"),
                        tlburttFilter.col("tp_ammortamento"), tlburttFilter.col("tp_rapporto"),
                        tlburttFilter.col("period_liquid"), tlburttFilter.col("cd_prodotto_ris"),
                        tlburttFilter.col("durata_originaria"), tlburttFilter.col("divisa"),
                        tlburttFilter.col("durata_residua"), tlburttFilter.col("tp_contr_rapp"));

        Dataset<Row> tlbcidefTlburtt = tlbcidefUrttPrinc.union(tlbcidefUrttColl).distinct();
        writeDatasetAsCsvAtPath(tlbcidefTlburtt, tlbcidefTlburttCsv);
    }
}
