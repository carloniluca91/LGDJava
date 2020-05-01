package it.carloni.luca.lgd.spark.steps;

import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import scala.collection.Seq;
import it.carloni.luca.lgd.spark.common.AbstractStep;
import it.carloni.luca.lgd.spark.utils.StepUtils;
import it.carloni.luca.lgd.schemas.FrappNdgMonthlySchema;

import java.util.Arrays;
import java.util.List;

import static it.carloni.luca.lgd.spark.utils.StepUtils.*;

public class FrappNdgMonthly extends AbstractStep {

    private final Logger logger = Logger.getLogger(FrappNdgMonthly.class);

    // required parameters
    private String dataA;
    private int numeroMesi1;
    private int numeroMesi2;

    public FrappNdgMonthly(String dataA, int numeroMesi1, int numeroMesi2){

        this.dataA = dataA;
        this.numeroMesi1 = numeroMesi1;
        this.numeroMesi2 = numeroMesi2;

        logger.debug("dataA: " + this.dataA);
        logger.debug("numeroMesi1: " + this.numeroMesi1);
        logger.debug("numeroMesi2: " + this.numeroMesi2);
    }

    @Override
    public void run() {

        String cicliNdgPathCsvPath = getValue("frapp.ndg.monthly.cicli.ndg.path.csv");
        String tlburttCsvPath = getValue("frapp.ndg.monthly.tlburtt.csv");
        String tlbcidefTlburttCsv = getValue("frapp.ndg.monthly.tlbcidef.tlburtt");

        logger.debug("cicliNdgPathCsvPath: " + cicliNdgPathCsvPath);
        logger.debug("tlburttCsvPath: " + tlburttCsvPath);
        logger.debug("tlbcidefTlburttCsv: " + tlbcidefTlburttCsv);

        // 26
        Dataset<Row> tlbcidef = readCsvAtPathUsingSchema(cicliNdgPathCsvPath,
                fromPigSchemaToStructType(FrappNdgMonthlySchema.getTlbcidefPigSchema()));
        // 53

        // 58
        Dataset<Row> cicliNdgPrinc = tlbcidef.filter(tlbcidef.col("cd_collegamento").isNull());
        Dataset<Row> cicliNdgColl = tlbcidef.filter(tlbcidef.col("cd_collegamento").isNotNull());
        // 60

        // 69
        Dataset<Row> tlburtt = readCsvAtPathUsingSchema(tlburttCsvPath,
                fromPigSchemaToStructType(FrappNdgMonthlySchema.getTlburttPigSchema()));
        // 107

        // 111
        Dataset<Row> tlburttFilter = tlburtt.filter(tlburtt.col("progr_segmento").equalTo(0));

        // ToDate((chararray)dt_riferimento,'yyyyMMdd') >= SubtractDuration(ToDate((chararray)datainiziodef,'yyyyMMdd'),'$numero_mesi_1')
        Column dtRiferimentoFilterPrincCol = tlburttFilter.col("dt_riferimento").geq(toIntCol(
                subtractDuration(StepUtils.toStringCol(cicliNdgPrinc.col("datainiziodef")), "yyyyMMdd", numeroMesi1)));

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
        Column dataACol = functions.lit(changeDateFormat(this.dataA, dataAPattern, "yyyyMMdd"));
        Column dataFineDefSubtractDurationPrincCol = subtractDuration(StepUtils.toStringCol(cicliNdgPrinc.col("datafinedef")), "yyyyMMdd", 1);
        Column leastDateDataFineDefDataAPrincCol = leastDate(dataFineDefSubtractDurationPrincCol, dataACol, "yyyyMMdd");
        Column addDurationLeastDateDataFineDefDataAPrincCol = addDuration(leastDateDataFineDefDataAPrincCol, "yyyyMMdd", numeroMesi2);

        // AND SUBSTRING( (chararray)dt_riferimento,0,6 ) <= SUBSTRING(AddDuration(...), 0, 6)
        Column dataFineDefFilterPrincCol = substringAndCastToInt(StepUtils.toStringCol(tlburttFilter.col("dt_riferimento")), 0, 6)
                .leq(substringAndCastToInt(addDurationLeastDateDataFineDefDataAPrincCol, 0, 6));

        // list of columns to be selected on cicliNdgPrinc
        List<String> cicliNdgPrincSelectColNames = Arrays.asList(
                "codicebanca", "ndgprincipale", "codicebanca_collegato",
                "ndg_collegato", "datainiziodef", "datafinedef");
        List<Column> tlbcidefUrttPrincCols = selectDfColumns(cicliNdgPrinc, cicliNdgPrincSelectColNames);

        // list of columns to be selected on tlburttFilter
        List<String> tlburttFilterSelectColNames = Arrays.asList("cd_istituto", "ndg", "sportello", "conto", "dt_riferimento",
                "conto_esteso", "forma_tecnica", "dt_accensione", "dt_estinzione", "dt_scadenza", "tp_ammortamento", "tp_rapporto",
                "period_liquid", "cd_prodotto_ris", "durata_originaria", "divisa", "durata_residua", "tp_contr_rapp");
        List<Column> tlburttFilterSelectCols = selectDfColumns(tlburttFilter, tlburttFilterSelectColNames);
        tlbcidefUrttPrincCols.addAll(tlburttFilterSelectCols);

        // conversion to scala Seq
        Seq<Column> tlbcidefUrttPrincColSeq = StepUtils.toScalaSeq(tlbcidefUrttPrincCols);
        Dataset<Row> tlbcidefUrttPrinc = cicliNdgPrinc.join(tlburttFilter, cicliNdgPrinc.col("codicebanca_collegato").equalTo(
                tlburttFilter.col("cd_istituto")).and(cicliNdgPrinc.col("ndg_collegato").equalTo(tlburttFilter.col("ndg"))))
                .filter(dtRiferimentoFilterPrincCol.and(dataFineDefFilterPrincCol))
                .select(tlbcidefUrttPrincColSeq);
        // 158

        // ToDate((chararray)dt_riferimento,'yyyyMMdd') >= SubtractDuration(ToDate((chararray)datainiziodef,'yyyyMMdd'),'$numero_mesi_1')
        Column dtRiferimentoFilterCollCol = tlburttFilter.col("dt_riferimento")
                .geq(subtractDuration(StepUtils.toStringCol(cicliNdgColl.col("datainiziodef")), "yyyyMMdd", numeroMesi1));

        /*
        AddDuration(ToDate((chararray)
            LeastDate((int)ToString(
                SubtractDuration(ToDate((chararray)datafinedef,'yyyyMMdd' ),'P1M'),'yyyyMMdd'),
                $data_a),
            'yyyyMMdd' ),'$numero_mesi_2' )
;
         */

        Column dataFineDefSubtractDurationCollCol = subtractDuration(StepUtils.toStringCol(cicliNdgColl.col("datafinedef")), "yyyyMMdd", 1);
        Column leastDateDataFineDefDataACollCol = leastDate(dataFineDefSubtractDurationCollCol, dataACol, "yyyyMMdd");
        Column addDurationLeastDateDataFineDefDataACollCol = addDuration(leastDateDataFineDefDataACollCol, "yyyyMMdd", numeroMesi2);

        // AND SUBSTRING( (chararray)dt_riferimento,0,6 ) <= SUBSTRING(AddDuration(...), 0, 6)
        Column dataFineDefFilterCollCol = substringAndCastToInt(StepUtils.toStringCol(tlburttFilter.col("dt_riferimento")), 0, 6)
                .leq(substringAndCastToInt(StepUtils.toStringCol(addDurationLeastDateDataFineDefDataACollCol), 0, 6));

        List<Column> tlbcidefUrttCollCols = selectDfColumns(cicliNdgColl, cicliNdgPrincSelectColNames);
        tlbcidefUrttCollCols.addAll(tlburttFilterSelectCols);

        Dataset<Row> tlbcidefUrttColl = cicliNdgColl.join(tlburttFilter, cicliNdgColl.col("codicebanca_collegato").equalTo(
                tlburttFilter.col("cd_istituto")).and(cicliNdgColl.col("ndg_collegato").equalTo(tlburttFilter.col("ndg"))))
                .filter(dtRiferimentoFilterCollCol.and(dataFineDefFilterCollCol))
                .select(StepUtils.toScalaSeq(tlbcidefUrttCollCols));

        Dataset<Row> tlbcidefTlburtt = tlbcidefUrttPrinc.union(tlbcidefUrttColl).distinct();
        writeDatasetAsCsvAtPath(tlbcidefTlburtt, tlbcidefTlburttCsv);
    }
}
