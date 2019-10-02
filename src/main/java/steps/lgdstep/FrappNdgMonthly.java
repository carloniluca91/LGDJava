package steps.lgdstep;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import steps.abstractstep.AbstractStep;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

public class FrappNdgMonthly extends AbstractStep {

    // required parameters
    private String dataA;
    private int numeroMesi1;
    private int numeroMesi2;

    public FrappNdgMonthly(String dataA, int numeroMesi1, int numeroMesi2){

        logger = Logger.getLogger(this.getClass().getName());

        this.dataA = dataA;
        this.numeroMesi1 = numeroMesi1;
        this.numeroMesi2 = numeroMesi2;

        stepInputDir = getProperty("frapp.ndg.monthly.input.dir");
        stepOutputDir = getProperty("frapp.ndg.monthly.output.dir");

        logger.info("stepInputDir: " + stepInputDir);
        logger.info("stepOutputDir: " + stepOutputDir);
        logger.info("dataA: " + this.dataA);
        logger.info("numeroMesi1:" + this.numeroMesi1);
        logger.info("numeroMesi2: " + this.numeroMesi2);
    }

    public void run() {

        String csvFormat = getProperty("csv.format");
        String cicliNdgPathCsv = getProperty("cicli.ndg.path.csv");
        logger.info("csvFormat: " + csvFormat);
        logger.info("cicliNdgPathCsv: " + cicliNdgPathCsv);

        // 26
        List<String> tlbcidefColumns = Arrays.asList("codicebanca", "ndgprincipale", "datainiziodef", "datafinedef", "datainiziopd",
                "datainizioristrutt", "datainizioinc", "datainiziosoff", "c_key", "tipo_segmne", "sae_segm", "rae_segm", "segmento",
                "tp_ndg", "provincia_segm", "databilseg", "strbilseg", "attivobilseg", "fatturbilseg", "ndg_collegato", "codicebanca_collegato",
                "cd_collegamento", "cd_fiscale", "dt_rif_udct");

        StructType tlbcidefSchema = getDfSchema(tlbcidefColumns);
        String cicliNdgPathCsvPath = Paths.get(stepInputDir, cicliNdgPathCsv).toString();
        logger.info("cicliNdgPathCsvPath: " + cicliNdgPathCsvPath);

        Dataset<Row> tlbcidef = sparkSession.read().format(csvFormat).option("delimiter", ",").schema(tlbcidefSchema).csv(cicliNdgPathCsvPath);
        // 53

        // 58
        Dataset<Row> cicliNdgPrinc = tlbcidef.filter(tlbcidef.col("cd_collegamento").isNull());
        Dataset<Row> cicliNdgColl = tlbcidef.filter(tlbcidef.col("cd_collegamento").isNotNull());
        // 60

        // 69
        List<String> tlburttColumns = Arrays.asList("cd_istituto", "ndg", "sportello", "conto", "progr_segmento", "dt_riferimento", "conto_esteso",
                "forma_tecnica", "flag_durata_contr", "cred_agevolato", "operazione_pool", "dt_accensione", "dt_estinzione", "dt_scadenza",
                "organo_deliber", "dt_delibera", "dt_scad_fido", "origine_rapporto", "tp_ammortamento", "tp_rapporto", "period_liquid",
                "freq_remargining", "cd_prodotto_ris", "durata_originaria", "divisa", "score_erogaz", "durata_residua", "categoria_sof",
                "categoria_inc", "dur_res_default", "flag_margine", "dt_entrata_def", "tp_contr_rapp", "cd_eplus", "r792_tipocartol");

        StructType tlburttSchema = getDfSchema(tlburttColumns);
        String tlburttCsv = getProperty("tlburtt.csv");
        String tlburttCsvPath = Paths.get(stepInputDir, tlburttCsv).toString();
        logger.info("tlburttCsv: " + tlburttCsv);
        logger.info("tlburttCsvPath: " + tlburttCsvPath);

        Dataset<Row> tlburtt = sparkSession.read().format(csvFormat).option("delimiter", ",").schema(tlburttSchema).csv(tlburttCsvPath);
        // 107

        // 111
        Dataset<Row> tlburttFilter = tlburtt.filter(castCol(tlburtt, "progr_segmento", DataTypes.IntegerType).equalTo(0));

        // ToDate((chararray)dt_riferimento,'yyyyMMdd') >= SubtractDuration(ToDate((chararray)datainiziodef,'yyyyMMdd'),'$numero_mesi_1')
        // dt_riferimento in format "yyyyMMdd", datainiziodef in format "yyyy-MM-dd" due to add_months
        Column dtRiferimentoFilterCol = getUnixTimeStampCol(tlburttFilter.col("dt_riferimento"), "yyyyMMdd").$greater$eq(
                getUnixTimeStampCol(functions.add_months(castToDateCol(cicliNdgPrinc.col("datainiziodef"),
                        "yyyyMMdd", "yyyy-MM-dd"), -numeroMesi1), "yyyy-MM-dd"));

        /*
        AddDuration(
            ToDate(
                (chararray)LeastDate(
                            (int)ToString(
                                    SubtractDuration(ToDate((chararray)
                                        datafinedef,'yyyyMMdd' ),
                                        'P1M'),
                                    'yyyyMMdd')
                            ,$data_a),
                'yyyyMMdd' ),
            '$numero_mesi_2' )
;
         */

        // dataFineDefCol in format "yyyy-MM-dd" due to add_months
        Column dataFineDefCol = functions.add_months(functions.from_unixtime(leastDate(
                // datafinedef -1 month in format "yyyy-MM-dd"
                functions.add_months(castToDateCol(cicliNdgPrinc.col("datafinedef"), "yyyyMMdd",
                        "yyyy-MM-dd"), -1),
                // dataA, already in format "yyyy-MM-dd"
                functions.lit(dataA), "yyyy-MM-dd"),
                // convert to StringType as we must apply SUBSTRING to it
                "yyyy-MM-dd"), numeroMesi2).cast(DataTypes.StringType);

        // dt_riferimento in format "yyyyMMdd", dataFineDefCol in format "yyyy-MM-dd" due to add_months
        Column dataFineDefFilterCol = getUnixTimeStampCol(functions.substring(tlburttFilter.col("dt_riferimento"), 0, 6),
                "yyyyMM").$less$eq(getUnixTimeStampCol(functions.substring(dataFineDefCol, 0, 7),"yyyy-MM"));

        // list of columns to be selected on cicliNdgPrinc
        List<String> cicliNdgPrincSelectColNames = Arrays.asList("codicebanca", "ndgprincipale", "codicebanca_collegato",
                "ndg_collegato", "datainiziodef", "datafinedef");
        List<Column> tlbcidefUrttPrincCols = selectDfColumns(cicliNdgPrinc, cicliNdgPrincSelectColNames);

        // list of columns to be selected on tlburttFilter
        List<String> tlburttFilterSelectColNames = Arrays.asList("cd_istituto", "ndg", "sportello", "conto", "dt_riferimento", "conto_esteso",
                "forma_tecnica", "dt_accensione", "dt_estinzione", "dt_scadenza", "tp_ammortamento", "tp_rapporto", "period_liquid", "cd_prodotto_ris",
                "durata_originaria", "divisa", "durata_residua", "tp_contr_rapp");
        List<Column> tlburttFilterSelectCols = selectDfColumns(tlburttFilter, tlburttFilterSelectColNames);
        tlbcidefUrttPrincCols.addAll(tlburttFilterSelectCols);

        // conversion to scala Seq
        Seq<Column> tlbcidefUrttPrincColSeq = JavaConverters.asScalaIteratorConverter(tlbcidefUrttPrincCols.iterator()).asScala().toSeq();
        Dataset<Row> tlbcidefUrttPrinc = cicliNdgPrinc.join(tlburttFilter, cicliNdgPrinc.col("codicebanca_collegato").equalTo(
                tlburttFilter.col("cd_istituto")).and(cicliNdgPrinc.col("ndg_collegato").equalTo(tlburttFilter.col("ndg"))))
                .filter(dtRiferimentoFilterCol.and(dataFineDefFilterCol))
                .select(tlbcidefUrttPrincColSeq);

        // 158

        // as tlbcidefUrttColl follows the same pipeline of tlbcidefUrttPrinc, except for the fact that
        // tlbcidefUrttColl uses cicliNdgColl as opposed to tlbcidefUrttPrinc that uses cicliNdgPrinc,
        // we simply slightly modify the previously defined column conditions and select columns

        // dt_riferimento in format "yyyyMMdd", datainiziodef in format "yyyy-MM-dd" due to add_months
        dtRiferimentoFilterCol = getUnixTimeStampCol(tlburttFilter.col("dt_riferimento"), "yyyyMMdd").$greater$eq(
                getUnixTimeStampCol(functions.add_months(castToDateCol(cicliNdgColl.col("datainiziodef"),
                        "yyyyMMdd", "yyyy-MM-dd"), -numeroMesi1), "yyyy-MM-dd"));

        // dataFineDefCol in format "yyyy-MM-dd" due to add_months
        dataFineDefCol = functions.add_months(functions.from_unixtime(leastDate(
                // datafinedef -1 in format "yyyy-MM-dd"
                functions.add_months(castToDateCol(cicliNdgColl.col("datafinedef"), "yyyyMMdd",
                        "yyyy-MM-dd"), -1),
                // dataA, already in format "yyyy-MM-dd"
                functions.lit(dataA), "yyyy-MM-dd"),
                "yyyy-MM-dd"), numeroMesi2).cast(DataTypes.StringType);

        // dt_riferimento in format "yyyyMMdd", dataFineDefCol in format "yyyy-MM-dd" due to add_months
        dataFineDefFilterCol = getUnixTimeStampCol(functions.substring(tlburttFilter.col("dt_riferimento"), 0, 6),
                "yyyyMM").$less$eq(getUnixTimeStampCol(functions.substring(dataFineDefCol, 0, 7),"yyyy-MM"));

        List<Column> tlbcidefUrttCollCols = selectDfColumns(cicliNdgColl, cicliNdgPrincSelectColNames);
        tlbcidefUrttCollCols.addAll(tlburttFilterSelectCols);

        // conversion to scala Seq
        Seq<Column> tlbcidefUrttCollColsSeq = JavaConverters.asScalaIteratorConverter(tlbcidefUrttCollCols.iterator()).asScala().toSeq();
        Dataset<Row> tlbcidefUrttColl = cicliNdgColl.join(tlburttFilter, cicliNdgColl.col("codicebanca_collegato").equalTo(
                tlburttFilter.col("cd_istituto")).and(cicliNdgColl.col("ndg_collegato").equalTo(tlburttFilter.col("ndg"))))
                .filter(dtRiferimentoFilterCol.and(dataFineDefFilterCol))
                .select(tlbcidefUrttCollColsSeq);

        Dataset<Row> tlbcidefTlburtt = tlbcidefUrttPrinc.union(tlbcidefUrttColl).distinct();

        String tlbcidefTlburttCsv = getProperty("tlbcidef.tlburtt");
        logger.info("tlbcidefTlburttCsv: " + tlbcidefTlburttCsv);

        logger.info("tlbcidefTlburtt count: " + tlbcidefTlburtt.count());
        tlbcidefTlburtt.write().format(csvFormat).option("delimiter", ",").mode(SaveMode.Overwrite).csv(
                Paths.get(stepOutputDir, tlbcidefTlburttCsv).toString());

    }
}
