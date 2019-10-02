package steps.lgdstep;

import org.apache.spark.sql.*;
import steps.abstractstep.AbstractStep;

import java.nio.file.Paths;
import java.util.*;
import java.util.logging.Logger;

public class FanagMonthly extends AbstractStep{

    private int numeroMesi1;
    private int numeroMesi2;
    private String dataA;

    public FanagMonthly(int numeroMesi1, int numeroMesi2, String dataA){

        logger = Logger.getLogger(this.getClass().getName());

        this.numeroMesi1 = numeroMesi1;
        this.numeroMesi2 = numeroMesi2;
        this.dataA = dataA;

        stepInputDir = getProperty("fanag.monthly.input.dir");
        stepOutputDir = getProperty("fanag.monthly.output.dir");

        logger.info("stepInputDir: " + stepInputDir);
        logger.info("stepOutputDir: " + stepOutputDir);
    }


    @Override
    public void run() {

        String csvFormat = getProperty("csv.format");
        String cicliNdgPath = getProperty("cicli.ndg.path.csv");
        String tlbuActPath = getProperty("tlbuact.csv");
        String tlbudtcPath = getProperty("tlbduct.path.csv");
        String fanagOutPath = getProperty("fanag.out");

        logger.info("csvFormat: " + csvFormat);
        logger.info("cicliNdgPath: " + cicliNdgPath);
        logger.info("tlbuActPath: " + tlbuActPath);
        logger.info("tlbudtcPath: " + tlbudtcPath);
        logger.info("fanagOutPath: " + fanagOutPath);

        List<String> cicliNdgColumnNames = Arrays.asList("codicebanca", "ndgprincipale", "datainiziodef", "datafinedef", "datainiziopd",
                "datainizioristrutt", "datainizioinc", "datainiziosoff", "c_key", "tipo_segmne", "sae_segm", "rae_segm", "segmento", "tp_ndg",
                "provincia_segm", "databilseg", "strbilseg", "attivobilseg", "fatturbilseg", "ndg_collegato", "codicebanca_collegato",
                "cd_collegamento", "cd_fiscale", "dt_rif_udct");

        Dataset<Row> cicliNdg = sparkSession.read().format(csvFormat).option("delimiter", ",").schema(getDfSchema(cicliNdgColumnNames)).csv(
                Paths.get(stepInputDir, cicliNdgPath).toString());

        // cicli_ndg_princ = FILTER cicli_ndg BY cd_collegamento IS NULL;
        // cicli_ndg_coll = FILTER cicli_ndg BY cd_collegamento IS NOT NULL;

        Dataset<Row> cicliNdgPrinc = cicliNdg.filter(cicliNdg.col("cd_collegamento").isNull());
        Dataset<Row> cicliNdgColl = cicliNdg.filter(cicliNdg.col("cd_collegamento").isNotNull());

        List<String> tlbuactColumnNames = Arrays.asList("cd_istituto", "ndg", "dt_riferimento", "cab_prov_com_stato", "provincia", "sae",
                "rae", "ciae", "sportello", "area_affari", "tp_ndg", "intestazione", "partita_iva", "cd_fiscale", "ctp_cred_proc_conc",
                "cliente_protestato", "status_sygei", "ndg_caponucleo", "specie_giuridica", "dt_inizio_rischio", "dt_revoca_fidi",
                "dt_cens_anagrafe", "cab_luogo_nascita", "dt_costituz_nascit", "tp_contr_basilea1", "flag_fallibile", "stato_controparte",
                "dt_estinz_ctp", "cliente_fido_rev", "tp_controparte", "grande_sett_attiv", "grande_ramo_attiv", "n058_interm_vigil",
                "n160_cod_ateco");

        List<String> tlbuactLoadSeletcColNames = Arrays.asList("dt_riferimento", "cd_istituto", "ndg", "tp_ndg", "intestazione",
                "cd_fiscale", "partita_iva", "sae", "rae", "ciae", "provincia", "sportello", "ndg_caponucleo");

        Dataset<Row> tlbuact = sparkSession.read().format(csvFormat).option("delimiter", ",").schema(getDfSchema(tlbuactColumnNames)).csv(
                Paths.get(stepInputDir, tlbuActPath).toString()).selectExpr(toScalaStringSeq(tlbuactLoadSeletcColNames));

        // JOIN  tlbuact BY (cd_istituto, ndg), cicli_ndg_princ BY (codicebanca_collegato, ndg_collegato);
        Column joinCondition = tlbuact.col("cd_istituto").equalTo(cicliNdgPrinc.col("codicebanca_collegato"))
                .and(tlbuact.col("ndg").equalTo(cicliNdgPrinc.col("ndg_collegato")));

        //  FILTER BY ToDate((chararray)dt_riferimento,'yyyyMMdd') >= SubtractDuration(ToDate((chararray)datainiziodef,'yyyyMMdd'),'$numero_mesi_1')
        Column dtRiferimentoDataInizioDefConditionCol = getUnixTimeStampCol(tlbuact.col("dt_riferimento"), "yyyyMMdd").geq(
                getUnixTimeStampCol(functions.add_months(castStringColToDateCol(cicliNdgPrinc.col("datainiziodef"), "yyyyMMdd"),
                        - numeroMesi1), "yyyy-MM-dd"));

        // LeastDate( (int)ToString(SubtractDuration(ToDate((chararray)datafinedef,'yyyyMMdd' ),'P1M'),'yyyyMMdd'), $data_a )
        Column leastDateCol = leastDate(
                functions.add_months(castStringColToDateCol(cicliNdgPrinc.col("datafinedef"), "yyyyMMdd"), -1),
                functions.lit(dataA), "yyyy-MM-dd");

        // AddDuration( ToDate( (chararray) leastDate(...),'yyyyMMdd'), $data_a ),'yyyyMMdd' ),'$numero_mesi_2' )
        Column leastDateAddDurationCol = functions.add_months(functions.from_unixtime(leastDateCol, "yyyy-MM-dd"), numeroMesi2);

        // SUBSTRING( (chararray)dt_riferimento,0,6 ) <= SUBSTRING(leastDateAddDurationCol, 0,6)
        Column dtRiferimentoLeastDateAddDurationConditionCol = getUnixTimeStampCol(
                functions.substring(tlbuact.col("dt_riferimento"), 0, 6), "yyyyMM")
                .leq(getUnixTimeStampCol(functions.date_format(leastDateAddDurationCol, "yyyy-MM"), "yyyy-MM"));

        // 132
        List<Column> tlbcidefTlbuactPrincSelectColList = new ArrayList<>(selectDfColumns(
                cicliNdg, Arrays.asList("codicebanca_collegato", "ndg_collegato", "datainiziodef", "datafinedef")));

        Map<String, String> selectColMap = new HashMap<>();
        selectColMap.put("tp_ndg", "naturagiuridica");
        selectColMap.put("intestazione", "datariferimento");
        selectColMap.put("cd_fiscale", "codicefiscale");
        selectColMap.put("sae", "sae");
        selectColMap.put("rae", "rae");
        selectColMap.put("ciae", "ciae");

        tlbcidefTlbuactPrincSelectColList.addAll(selectDfColumns(tlbuact, selectColMap));
        tlbcidefTlbuactPrincSelectColList.add(cicliNdgPrinc.col("provincia_segm").alias("provincia"));
        tlbcidefTlbuactPrincSelectColList.add(tlbuact.col("provincia").alias("provincia_cod"));
        tlbcidefTlbuactPrincSelectColList.addAll(selectDfColumns(tlbuact, Collections.singletonList("sportello")));
        tlbcidefTlbuactPrincSelectColList.addAll(selectDfColumns(cicliNdgPrinc, Arrays.asList("segmento", "cd_collegamento")));
        tlbcidefTlbuactPrincSelectColList.add(tlbuact.col("ndg_caponucleo"));
        tlbcidefTlbuactPrincSelectColList.addAll(selectDfColumns(cicliNdgPrinc, Arrays.asList("codicebanca", "ndgprincipale")));

        // 154

        Dataset<Row> tlbcidefTlbuactPrinc = tlbuact.join(cicliNdgPrinc, joinCondition, "inner")
                .filter(dtRiferimentoDataInizioDefConditionCol.and(dtRiferimentoLeastDateAddDurationConditionCol))
                .select(toScalaColSeq(tlbcidefTlbuactPrincSelectColList));

        // JOIN  tlbuact BY (cd_istituto, ndg), cicli_ndg_coll BY (codicebanca_collegato, ndg_collegato);
        joinCondition = tlbuact.col("cd_istituto").equalTo(cicliNdgColl.col("codicebanca_collegato"))
                .and(tlbuact.col("ndg").equalTo(cicliNdgColl.col("ndg_collegato")));

        //  FILTER BY ToDate((chararray)dt_riferimento,'yyyyMMdd') >= SubtractDuration(ToDate((chararray)datainiziodef,'yyyyMMdd'),'$numero_mesi_1')
        dtRiferimentoDataInizioDefConditionCol = getUnixTimeStampCol(tlbuact.col("dt_riferimento"), "yyyyMMdd").geq(
                getUnixTimeStampCol(functions.add_months(castStringColToDateCol(cicliNdgColl.col("datainiziodef"), "yyyyMMdd"),
                        - numeroMesi1), "yyyy-MM-dd"));

        // LeastDate( (int)ToString(SubtractDuration(ToDate((chararray)datafinedef,'yyyyMMdd' ),'P1M'),'yyyyMMdd'), $data_a )
        leastDateCol = leastDate(functions.add_months(castStringColToDateCol(cicliNdgColl.col("datafinedef"), "yyyyMMdd"), -1),
                functions.lit(dataA), "yyyy-MM-dd");

        // AddDuration( ToDate( (chararray) leastDate(...),'yyyyMMdd'), $data_a ),'yyyyMMdd' ),'$numero_mesi_2' )
        leastDateAddDurationCol = functions.add_months(functions.from_unixtime(leastDateCol, "yyyy-MM-dd"), numeroMesi2);

        // SUBSTRING( (chararray)dt_riferimento,0,6 ) <= SUBSTRING(leastDateAddDurationCol, 0,6)
        dtRiferimentoLeastDateAddDurationConditionCol = getUnixTimeStampCol(
                functions.substring(tlbuact.col("dt_riferimento"), 0, 6), "yyyyMM")
                .leq(getUnixTimeStampCol(functions.date_format(leastDateAddDurationCol, "yyyy-MM"), "yyyy-MM"));

        // 132
        List<Column> tlbcidefTlbuactCollSelectColList = new ArrayList<>(selectDfColumns(
                cicliNdg, Arrays.asList("codicebanca_collegato", "ndg_collegato", "datainiziodef", "datafinedef")));

        selectColMap = new HashMap<>();
        selectColMap.put("tp_ndg", "naturagiuridica");
        selectColMap.put("intestazione", "datariferimento");
        selectColMap.put("cd_fiscale", "codicefiscale");
        selectColMap.put("sae", "sae");
        selectColMap.put("rae", "rae");
        selectColMap.put("ciae", "ciae");

        tlbcidefTlbuactCollSelectColList.addAll(selectDfColumns(tlbuact, selectColMap));
        tlbcidefTlbuactCollSelectColList.add(cicliNdgColl.col("provincia_segm").alias("provincia"));
        tlbcidefTlbuactCollSelectColList.add(tlbuact.col("provincia").alias("provincia_cod"));
        tlbcidefTlbuactCollSelectColList.addAll(selectDfColumns(tlbuact, Collections.singletonList("sportello")));
        tlbcidefTlbuactCollSelectColList.addAll(selectDfColumns(cicliNdgColl, Arrays.asList("segmento", "cd_collegamento")));
        tlbcidefTlbuactCollSelectColList.add(tlbuact.col("ndg_caponucleo"));
        tlbcidefTlbuactCollSelectColList.addAll(selectDfColumns(cicliNdgColl, Arrays.asList("codicebanca", "ndgprincipale")));

        Dataset<Row> tlbcidefTlbuactColl = tlbuact.join(cicliNdgPrinc, joinCondition, "inner")
                .filter(dtRiferimentoDataInizioDefConditionCol.and(dtRiferimentoLeastDateAddDurationConditionCol))
                .select(toScalaColSeq(tlbcidefTlbuactCollSelectColList));

        Dataset<Row> tlbcidefTlbuact = tlbcidefTlbuactPrinc.union(tlbcidefTlbuactColl).distinct();

        List<String> tlbudtcColumnNames = Arrays.asList("cd_istituto", "ndg", "dt_riferimento", "reddito", "totale_utilizzi", "totale_fidi_delib",
                "totale_accordato", "patrimonio", "n_dipendenti", "tot_rischi_indir", "status_ndg", "status_basilea2", "posiz_soff_inc",
                "dubb_esito_inc_ndg", "status_cliente_lab", "tot_util_mortgage", "tot_acco_mortgage", "dt_entrata_default", "cd_stato_def_t0",
                "cd_tipo_def_t0", "cd_stato_def_a_t12", "cd_rap_ristr", "tp_ristrutt", "tp_cli_scad_scf", "tp_cli_scad_scf_b2", "totale_saldi_0063",
                "totale_saldi_0260");

        Dataset<Row> tlbudtc = sparkSession.read().format(csvFormat).option("delimiter", ",").schema(getDfSchema(tlbudtcColumnNames))
                .csv(Paths.get(stepInputDir, tlbudtcPath).toString());

        // JOIN  tlbudtc BY (cd_istituto, ndg, dt_riferimento), tlbcidef_tlbuact BY (codicebanca_collegato,ndg_collegato, datariferimento) ;
        joinCondition = tlbudtc.col("cd_istituto").equalTo(tlbcidefTlbuact.col("codicebanca_collegato"))
                .and(tlbudtc.col("ndg").equalTo(tlbcidefTlbuact.col("ndg_collegato")))
                .and(tlbudtc.col("dt_riferimento").equalTo(tlbcidefTlbuact.col("datariferimento")));

        List<Column> fanagOutSelectColList = new ArrayList<>(selectDfColumns(
                tlbcidefTlbuact, Arrays.asList("codicebanca_collegato", "ndg_collegato", "datariferimento")));

        fanagOutSelectColList.addAll(selectDfColumns(tlbudtc, Arrays.asList("totale_accordato", "totale_utilizzi", "tot_acco_mortgage",
                "tot_util_mortgage", "totale_saldi_0063", "totale_saldi_0260")));
        fanagOutSelectColList.addAll(selectDfColumns(tlbcidefTlbuact, Arrays.asList("naturagiuridica", "intestazione",
                "codicefiscale", "partitaiva ", "sae", "rae", "ciae", "provincia", "provincia_cod", "sportello")));
        fanagOutSelectColList.addAll(selectDfColumns(tlbudtc, Arrays.asList("posiz_soff_inc", "status_ndg", "tp_cli_scad_scf","cd_rap_ristr")));
        fanagOutSelectColList.addAll(selectDfColumns(tlbcidefTlbuact, Arrays.asList("segmento", "cd_collegamento", "ndg_caponucleo")));

        // (tlbudtc::tp_ristrutt != '0' ? 'S' : 'N') as flag_ristrutt
        Column flagRistruttCol = functions.when(tlbudtc.col("tp_ristrutt").notEqual(0), "S").otherwise("N");
        fanagOutSelectColList.add(flagRistruttCol);

        fanagOutSelectColList.addAll(selectDfColumns(tlbcidefTlbuact, Arrays.asList("codicebanca", "ndgprincipale", "datainiziodef")));

        Dataset<Row> fanagOut = tlbudtc.join(tlbcidefTlbuact, joinCondition, "inner").select(toScalaColSeq(fanagOutSelectColList));

        fanagOut.write().format(csvFormat).option("delimiter", ",").mode(SaveMode.Overwrite)
                .csv(Paths.get(stepOutputDir, fanagOutPath).toString());
    }
}
