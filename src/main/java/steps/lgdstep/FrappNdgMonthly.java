package steps.lgdstep;

import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.Seq;
import steps.abstractstep.AbstractStep;
import steps.schemas.FrappNdgMonthlySchema;

import java.util.Arrays;
import java.util.List;

import static steps.abstractstep.StepUtils.*;

public class FrappNdgMonthly extends AbstractStep {

    // required parameters
    private String dataA;
    private int numeroMesi1;
    private int numeroMesi2;

    public FrappNdgMonthly(String dataA, int numeroMesi1, int numeroMesi2){

        logger = Logger.getLogger(FanagMonthly.class);

        this.dataA = dataA;
        this.numeroMesi1 = numeroMesi1;
        this.numeroMesi2 = numeroMesi2;

        stepInputDir = getLGDPropertyValue("frapp.ndg.monthly.input.dir");
        stepOutputDir = getLGDPropertyValue("frapp.ndg.monthly.output.dir");

        logger.debug("dataA: " + this.dataA);
        logger.debug("numeroMesi1: " + this.numeroMesi1);
        logger.debug("numeroMesi2: " + this.numeroMesi2);
        logger.debug("stepInputDir: " + stepInputDir);
        logger.debug("stepOutputDir: " + stepOutputDir);
    }

    @Override
    public void run() {

        String cicliNdgPathCsvPath = getLGDPropertyValue("frapp.ndg.monthly.cicli.ndg.path.csv");
        String tlburttCsvPath = getLGDPropertyValue("frapp.ndg.monthly.tlburtt.csv");
        String tlbcidefTlburttCsv = getLGDPropertyValue("frapp.ndg.monthly.tlbcidef.tlburtt");

        logger.debug("cicliNdgPathCsvPath: " + cicliNdgPathCsvPath);
        logger.debug("tlburttCsvPath: " + tlburttCsvPath);
        logger.debug("tlbcidefTlburttCsv: " + tlbcidefTlburttCsv);

        // 26
        Dataset<Row> tlbcidef = sparkSession.read().format(csvFormat).option("delimiter", ",")
                .schema(fromPigSchemaToStructType(FrappNdgMonthlySchema.getTlbcidefPigSchema()))
                .csv(cicliNdgPathCsvPath);
        // 53

        // 58
        Dataset<Row> cicliNdgPrinc = tlbcidef.filter(tlbcidef.col("cd_collegamento").isNull());
        Dataset<Row> cicliNdgColl = tlbcidef.filter(tlbcidef.col("cd_collegamento").isNotNull());
        // 60

        // 69
        Dataset<Row> tlburtt = sparkSession.read().format(csvFormat).option("delimiter", ",")
                .schema(fromPigSchemaToStructType(FrappNdgMonthlySchema.getTlburttPigSchema()))
                .csv(tlburttCsvPath);
        // 107

        // 111
        Dataset<Row> tlburttFilter = tlburtt.filter(tlburtt.col("progr_segmento").cast(DataTypes.IntegerType).equalTo(0));

        // ToDate((chararray)dt_riferimento,'yyyyMMdd') >= SubtractDuration(ToDate((chararray)datainiziodef,'yyyyMMdd'),'$numero_mesi_1')
        Column dtRiferimentoFilterPrincCol = tlburttFilter.col("dt_riferimento")
                .geq(subtractDuration(cicliNdgPrinc.col("datainiziodef"), "yyyyMMdd", numeroMesi1));

        /*
        AddDuration(ToDate((chararray)
            LeastDate((int)ToString(
                SubtractDuration(ToDate((chararray)datafinedef,'yyyyMMdd' ),'P1M'),'yyyyMMdd'),
                $data_a),
            'yyyyMMdd' ),'$numero_mesi_2' )
;
         */

        // we need to format $data_a from yyyy-MM-dd to yyyyMMdd
        String dataAPattern = getLGDPropertyValue("params.dataa.pattern");
        Column dataACol = functions.lit(changeDateFormat(this.dataA, dataAPattern, "yyyyMMdd"));
        Column dataFineDefSubtractDurationPrincCol = subtractDuration(cicliNdgPrinc.col("datafinedef"), "yyyyMMdd", 1);
        Column leastDateDataFineDefDataAPrincCol = leastDate(dataFineDefSubtractDurationPrincCol, dataACol, "yyyyMMdd");
        Column addDurationLeastDateDataFineDefDataAPrincCol = addDuration(leastDateDataFineDefDataAPrincCol, "yyyyMMdd", numeroMesi2);

        // AND SUBSTRING( (chararray)dt_riferimento,0,6 ) <= SUBSTRING(AddDuration(...), 0, 6)
        Column dataFineDefFilterPrincCol = substringAndCastToInt(tlburttFilter.col("dt_riferimento"), 0, 6).leq(
                substringAndCastToInt(addDurationLeastDateDataFineDefDataAPrincCol, 0, 6));

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
        Seq<Column> tlbcidefUrttPrincColSeq = toScalaColSeq(tlbcidefUrttPrincCols);
        Dataset<Row> tlbcidefUrttPrinc = cicliNdgPrinc.join(tlburttFilter, cicliNdgPrinc.col("codicebanca_collegato").equalTo(
                tlburttFilter.col("cd_istituto")).and(cicliNdgPrinc.col("ndg_collegato").equalTo(tlburttFilter.col("ndg"))))
                .filter(dtRiferimentoFilterPrincCol.and(dataFineDefFilterPrincCol))
                .select(tlbcidefUrttPrincColSeq);

        // 158

        // ToDate((chararray)dt_riferimento,'yyyyMMdd') >= SubtractDuration(ToDate((chararray)datainiziodef,'yyyyMMdd'),'$numero_mesi_1')
        Column dtRiferimentoFilterCollCol = tlburttFilter.col("dt_riferimento")
                .geq(subtractDuration(cicliNdgColl.col("datainiziodef"), "yyyyMMdd", numeroMesi1));

        /*
        AddDuration(ToDate((chararray)
            LeastDate((int)ToString(
                SubtractDuration(ToDate((chararray)datafinedef,'yyyyMMdd' ),'P1M'),'yyyyMMdd'),
                $data_a),
            'yyyyMMdd' ),'$numero_mesi_2' )
;
         */

        Column dataFineDefSubtractDurationCollCol = subtractDuration(cicliNdgColl.col("datafinedef"), "yyyyMMdd", 1);
        Column leastDateDataFineDefDataACollCol = leastDate(dataFineDefSubtractDurationCollCol, dataACol, "yyyyMMdd");
        Column addDurationLeastDateDataFineDefDataACollCol = addDuration(leastDateDataFineDefDataACollCol, "yyyyMMdd", numeroMesi2);

        // AND SUBSTRING( (chararray)dt_riferimento,0,6 ) <= SUBSTRING(AddDuration(...), 0, 6)
        Column dataFineDefFilterCollCol = substringAndCastToInt(tlburttFilter.col("dt_riferimento"), 0, 6).leq(
                substringAndCastToInt(addDurationLeastDateDataFineDefDataACollCol, 0, 6));

        List<Column> tlbcidefUrttCollCols = selectDfColumns(cicliNdgColl, cicliNdgPrincSelectColNames);
        tlbcidefUrttCollCols.addAll(tlburttFilterSelectCols);

        Dataset<Row> tlbcidefUrttColl = cicliNdgColl.join(tlburttFilter, cicliNdgColl.col("codicebanca_collegato").equalTo(
                tlburttFilter.col("cd_istituto")).and(cicliNdgColl.col("ndg_collegato").equalTo(tlburttFilter.col("ndg"))))
                .filter(dtRiferimentoFilterCollCol.and(dataFineDefFilterCollCol))
                .select(toScalaColSeq(tlbcidefUrttCollCols));

        Dataset<Row> tlbcidefTlburtt = tlbcidefUrttPrinc.union(tlbcidefUrttColl).distinct();

        tlbcidefTlburtt.write().format(csvFormat).option("delimiter", ",").mode(SaveMode.Overwrite).csv(tlbcidefTlburttCsv);
    }
}
