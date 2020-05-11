package it.carloni.luca.lgd.spark.step;

import it.carloni.luca.lgd.parameter.step.DataANumeroMesi12Value;
import it.carloni.luca.lgd.spark.common.AbstractStep;
import it.carloni.luca.lgd.schema.FrappNdgMonthlySchema;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import static it.carloni.luca.lgd.spark.utils.StepUtils.addDurationUDF;
import static it.carloni.luca.lgd.spark.utils.StepUtils.subtractDurationUDF;
import static it.carloni.luca.lgd.spark.utils.StepUtils.changeDateFormat;
import static it.carloni.luca.lgd.spark.utils.StepUtils.leastDateUDF;
import static it.carloni.luca.lgd.spark.utils.StepUtils.toStringCol;

public class FrappNdgMonthly extends AbstractStep<DataANumeroMesi12Value> {

    private final Logger logger = Logger.getLogger(getClass());

    @Override
    public void run(DataANumeroMesi12Value stepValues) {

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

        String YYYYMMDDFormat = "yyyyMMdd";

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
        Column dtRiferimentoFilterPrincCol = toStringCol(tlburttFilter.col("dt_riferimento"))
                .geq(subtractDurationUDF(toStringCol(cicliNdgPrinc.col("datainiziodef")), YYYYMMDDFormat, numeroMesi1));

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
        Column dataACol = functions.lit(changeDateFormat(dataA, dataAPattern, YYYYMMDDFormat));
        Column dataFineDefSubtractDurationPrincCol = subtractDurationUDF(toStringCol(cicliNdgPrinc.col("datafinedef")), YYYYMMDDFormat, 1);
        Column leastDateDataFineDefDataAPrincCol = leastDateUDF(dataFineDefSubtractDurationPrincCol, dataACol, YYYYMMDDFormat);
        Column addDurationLeastDateDataFineDefDataAPrincCol = addDurationUDF(leastDateDataFineDefDataAPrincCol, YYYYMMDDFormat, numeroMesi2);

        // AND SUBSTRING( (chararray)dt_riferimento,0,6 ) <= SUBSTRING(AddDuration(...), 0, 6)
        Column dataFineDefFilterPrincCol = substring06(tlburttFilter.col("dt_riferimento"))
                .leq(substring06(addDurationLeastDateDataFineDefDataAPrincCol));

        Column tlbcidefUrttPrincJoinCondition = cicliNdgPrinc.col("codicebanca_collegato").equalTo(tlburttFilter.col("cd_istituto"))
                .and(cicliNdgPrinc.col("ndg_collegato").equalTo(tlburttFilter.col("ndg")));

        Dataset<Row> tlbcidefUrttPrinc = cicliNdgPrinc.join(tlburttFilter, tlbcidefUrttPrincJoinCondition)
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
        Column dtRiferimentoFilterCollCol = toStringCol(tlburttFilter.col("dt_riferimento"))
                .geq(subtractDurationUDF(toStringCol(cicliNdgColl.col("datainiziodef")), YYYYMMDDFormat, numeroMesi1));

        /*
        AddDuration(ToDate((chararray)
            LeastDate((int)ToString(
                SubtractDuration(ToDate((chararray)datafinedef,'yyyyMMdd' ),'P1M'),'yyyyMMdd'),
                $data_a),
            'yyyyMMdd' ),'$numero_mesi_2' )
;
         */

        Column dataFineDefSubtractDurationCollCol = subtractDurationUDF(toStringCol(cicliNdgColl.col("datafinedef")), YYYYMMDDFormat, 1);
        Column leastDateDataFineDefDataACollCol = leastDateUDF(dataFineDefSubtractDurationCollCol, dataACol, YYYYMMDDFormat);
        Column addDurationLeastDateDataFineDefDataACollCol = addDurationUDF(leastDateDataFineDefDataACollCol, YYYYMMDDFormat, numeroMesi2);

        // AND SUBSTRING( (chararray)dt_riferimento,0,6 ) <= SUBSTRING(AddDuration(...), 0, 6)
        Column dataFineDefFilterCollCol = substring06(tlburttFilter.col("dt_riferimento"))
                .leq(substring06(addDurationLeastDateDataFineDefDataACollCol));

        Column tlbcidefUrttCollJoinCondition = cicliNdgColl.col("codicebanca_collegato").equalTo(tlburttFilter.col("cd_istituto"))
                .and(cicliNdgColl.col("ndg_collegato").equalTo(tlburttFilter.col("ndg")));

        Dataset<Row> tlbcidefUrttColl = cicliNdgColl.join(tlburttFilter, tlbcidefUrttCollJoinCondition)
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

        Dataset<Row> tlbcidefTlburtt = tlbcidefUrttPrinc
                .union(tlbcidefUrttColl)
                .distinct();

        writeDatasetAsCsvAtPath(tlbcidefTlburtt, tlbcidefTlburttCsv);
    }

    private Column substring06(Column column) {

        return functions.substring(column.cast(DataTypes.StringType), 0, 6);
    }
}
