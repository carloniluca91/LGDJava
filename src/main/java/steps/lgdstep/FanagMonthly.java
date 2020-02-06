package steps.lgdstep;

import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import steps.abstractstep.AbstractStep;
import steps.schemas.FanagMonthlySchema;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static steps.abstractstep.StepUtils.*;

public class FanagMonthly extends AbstractStep {

    private int numeroMesi1;
    private int numeroMesi2;
    private String dataA;

    public FanagMonthly(int numeroMesi1, int numeroMesi2, String dataA){

        logger = Logger.getLogger(FanagMonthly.class);

        this.numeroMesi1 = numeroMesi1;
        this.numeroMesi2 = numeroMesi2;
        this.dataA = dataA;

        stepInputDir = getLGDPropertyValue("fanag.monthly.input.dir");
        stepOutputDir = getLGDPropertyValue("fanag.monthly.output.dir");

        logger.debug("stepInputDir: " + stepInputDir);
        logger.debug("stepOutputDir: " + stepOutputDir);
    }

    public void run() {

        String cicliNdgPath = getLGDPropertyValue("fanag.monthly.cicli.ndg.path.csv");
        String tlbuActPath = getLGDPropertyValue("fanag.monthly.tlbuact.csv");
        String tlbudtcPath = getLGDPropertyValue("fanag.monthly.tlbduct.path.csv");
        String fanagOutPath = getLGDPropertyValue("fanag.monthly.fanag.out");

        logger.debug("cicliNdgPath: " + cicliNdgPath);
        logger.debug("tlbuActPath: " + tlbuActPath);
        logger.debug("tlbudtcPath: " + tlbudtcPath);
        logger.debug("fanagOutPath: " + fanagOutPath);

        Dataset<Row> cicliNdg = sparkSession.read().format(csvFormat).option("delimiter", ",")
                .schema(fromPigSchemaToStructType(FanagMonthlySchema.getCicliNdgPigSchema()))
                .csv(Paths.get(stepInputDir, cicliNdgPath).toString());

        // cicli_ndg_princ = FILTER cicli_ndg BY cd_collegamento IS NULL;
        // cicli_ndg_coll = FILTER cicli_ndg BY cd_collegamento IS NOT NULL;

        Dataset<Row> cicliNdgPrinc = cicliNdg.filter(cicliNdg.col("cd_collegamento").isNull());
        Dataset<Row> cicliNdgColl = cicliNdg.filter(cicliNdg.col("cd_collegamento").isNotNull());

        Dataset<Row> tlbuact = sparkSession.read().format(csvFormat).option("delimiter", ",")
                .schema(fromPigSchemaToStructType(FanagMonthlySchema.getTlbuactPigSchema()))
                .csv(tlbuActPath)
                .selectExpr("dt_riferimento", "cd_istituto", "ndg", "tp_ndg", "intestazione",
                        "cd_fiscale", "partita_iva", "sae", "rae", "ciae", "provincia", "sportello", "ndg_caponucleo");

        // JOIN  tlbuact BY (cd_istituto, ndg), cicli_ndg_princ BY (codicebanca_collegato, ndg_collegato);
        Column tlbuactCicliNdgPrincJoinCondition = tlbuact.col("cd_istituto").equalTo(cicliNdgPrinc.col("codicebanca_collegato"))
                .and(tlbuact.col("ndg").equalTo(cicliNdgPrinc.col("ndg_collegato")));

        //  FILTER BY ToDate((chararray)dt_riferimento,'yyyyMMdd') >= SubtractDuration(ToDate((chararray)datainiziodef,'yyyyMMdd'),'$numero_mesi_1')
        Column dtRiferimentoDataInizioDefPrincConditionCol = tlbuact.col("dt_riferimento").cast(DataTypes.IntegerType)
                .geq(subtractDuration(cicliNdgPrinc.col("datainiziodef"), "yyyyMMdd", numeroMesi1).cast(DataTypes.IntegerType));

        // LeastDate( (int)ToString(SubtractDuration(ToDate((chararray)datafinedef,'yyyyMMdd' ),'P1M'),'yyyyMMdd'), $data_a )
        // [a] SubtractDuration(ToDate((chararray)datafinedef,'yyyyMMdd' ),'P1M'),'yyyyMMdd')
        Column cicliNdgPrincDataFineDefSubtractDurationCol = subtractDuration(cicliNdgPrinc.col("datafinedef"), "yyyyMMdd", 1);

        // we need to format $data_a from yyyy-MM-dd to yyyyMMdd
        String dataAPattern = getLGDPropertyValue("params.dataa.pattern");
        Column dataACol = functions.lit(changeDateFormat(this.dataA, dataAPattern, "yyyyMMdd"));
        Column leastDatePrincCol = leastDate(cicliNdgPrincDataFineDefSubtractDurationCol, dataACol, "yyyyMMdd");

        // AddDuration( ToDate( (chararray) leastDate(...),'yyyyMMdd'), $data_a ),'yyyyMMdd' ),'$numero_mesi_2' )
        Column leastDateAddDurationPrincCol = addDuration(leastDatePrincCol, "yyyyMMdd", numeroMesi2);

        // SUBSTRING( (chararray)dt_riferimento,0,6 ) <= SUBSTRING(leastDateAddDurationPrincCol, 0,6)
        Column dtRiferimentoLeastDateAddDurationPrincConditionCol =
                substringAndCastToInt(tlbuact.col("dt_riferimento"), 0, 6)
                        .leq(substringAndCastToInt(leastDateAddDurationPrincCol, 0, 6));

        // 132

        // 154

        /*
          cicli_ndg_princ::codicebanca_collegato as codicebanca_collegato
         ,cicli_ndg_princ::ndg_collegato  as ndg_collegato
         ,cicli_ndg_princ::datainiziodef  as datainiziodef
         ,cicli_ndg_princ::datafinedef  as datafinedef
         ,tlbuact::dt_riferimento as datariferimento
         ,tlbuact::tp_ndg as naturagiuridica
         ,tlbuact::intestazione as intestazione
         ,tlbuact::cd_fiscale as codicefiscale
         ,tlbuact::partita_iva as partitaiva
         ,tlbuact::sae as sae
         ,tlbuact::rae as rae
         ,tlbuact::ciae as ciae
         ,cicli_ndg_princ::provincia_segm as  provincia
         ,tlbuact::provincia as provincia_cod
         ,tlbuact::sportello as  sportello
         ,cicli_ndg_princ::segmento as segmento
         ,cicli_ndg_princ::cd_collegamento as cd_collegamento
         ,tlbuact::ndg_caponucleo as ndg_caponucleo
         ,cicli_ndg_princ::codicebanca  as codicebanca
         ,cicli_ndg_princ::ndgprincipale  as ndgprincipale
         */

        Dataset<Row> tlbcidefTlbuactPrinc = tlbuact.join(cicliNdgPrinc, tlbuactCicliNdgPrincJoinCondition, "inner")
                .filter(dtRiferimentoDataInizioDefPrincConditionCol.and(dtRiferimentoLeastDateAddDurationPrincConditionCol))
                .select(cicliNdgPrinc.col("codicebanca_collegato"),
                        cicliNdgPrinc.col("ndg_collegato"),
                        cicliNdgPrinc.col("datainiziodef"),
                        cicliNdgPrinc.col("datafinedef"),
                        tlbuact.col("dt_riferimento").alias("datariferimento"),
                        tlbuact.col("tp_ndg").alias("natura_giuridica"),
                        tlbuact.col("intestazione"),
                        tlbuact.col("cd_fiscale").alias("codice_fiscale"),
                        tlbuact.col("partita_iva").alias("partitaiva"),
                        tlbuact.col("sae"), tlbuact.col("rae"), tlbuact.col("ciae"),
                        cicliNdgPrinc.col("provincia_segm").alias("provincia"),
                        tlbuact.col("provincia").alias("provincia_cod"),
                        tlbuact.col("sportello"),
                        cicliNdgPrinc.col("segmento"),
                        cicliNdgPrinc.col("cd_collegamento"),
                        tlbuact.col("ndg_caponucleo"),
                        cicliNdgPrinc.col("codicebanca"),
                        cicliNdgPrinc.col("ndgprincipale"));

        // JOIN  tlbuact BY (cd_istituto, ndg), cicli_ndg_coll BY (codicebanca_collegato, ndg_collegato);
        Column tlbuactCicliNdgCollJoinCondition = tlbuact.col("cd_istituto").equalTo(cicliNdgColl.col("codicebanca_collegato"))
                .and(tlbuact.col("ndg").equalTo(cicliNdgColl.col("ndg_collegato")));

        //  FILTER BY ToDate((chararray)dt_riferimento,'yyyyMMdd') >= SubtractDuration(ToDate((chararray)datainiziodef,'yyyyMMdd'),'$numero_mesi_1')
        Column dtRiferimentoDataInizioDefCollConditionCol = tlbuact.col("dt_riferimento").cast(DataTypes.IntegerType).geq(
                subtractDuration(cicliNdgColl.col("datainiziodef"),"yyyyMMdd", numeroMesi1).cast(DataTypes.IntegerType));

        // LeastDate( (int)ToString(SubtractDuration(ToDate((chararray)datafinedef,'yyyyMMdd' ),'P1M'),'yyyyMMdd'), $data_a )
        Column cicliNdgCollDataFineDefSubtractDurationCol = subtractDuration(cicliNdgColl.col("datafinedef"), "yyyyMMdd", 1);
        Column leastDateCollCol = leastDate(cicliNdgCollDataFineDefSubtractDurationCol, dataACol, "yyyyMMdd");

        // AddDuration( ToDate( (chararray) leastDate(...),'yyyyMMdd'), $data_a ),'yyyyMMdd' ),'$numero_mesi_2' )
        Column leastDateAddDurationCollCol = addDuration(leastDateCollCol, "yyyyMMdd", numeroMesi2);

        // SUBSTRING( (chararray)dt_riferimento,0,6 ) <= SUBSTRING(leastDateAddDurationPrincCol, 0,6)
        Column dtRiferimentoLeastDateAddDurationCollConditionCol =
                substringAndCastToInt(tlbuact.col("dt_riferimento"), 0, 6)
                        .leq(substringAndCastToInt(leastDateAddDurationCollCol, 0, 6));

        // 132

        /*
         cicli_ndg_coll::codicebanca_collegato as codicebanca_collegato
         ,cicli_ndg_coll::ndg_collegato  as ndg_collegato
         ,cicli_ndg_coll::datainiziodef  as datainiziodef
         ,cicli_ndg_coll::datafinedef  as datafinedef
         ,tlbuact::dt_riferimento as datariferimento
         ,tlbuact::tp_ndg as naturagiuridica
         ,tlbuact::intestazione as intestazione
         ,tlbuact::cd_fiscale as codicefiscale
         ,tlbuact::partita_iva as partitaiva
         ,tlbuact::sae as sae
         ,tlbuact::rae as rae
         ,tlbuact::ciae as ciae
         ,cicli_ndg_coll::provincia_segm as  provincia
         ,tlbuact::provincia as provincia_cod
         ,tlbuact::sportello as  sportello
         ,cicli_ndg_coll::segmento as segmento
         ,cicli_ndg_coll::cd_collegamento as cd_collegamento
         ,tlbuact::ndg_caponucleo as ndg_caponucleo
         ,cicli_ndg_coll::codicebanca  as codicebanca
         ,cicli_ndg_coll::ndgprincipale  as ndgprincipale
         */

        Dataset<Row> tlbcidefTlbuactColl = tlbuact.join(cicliNdgColl, tlbuactCicliNdgCollJoinCondition, "inner")
                .filter(dtRiferimentoDataInizioDefCollConditionCol.and(dtRiferimentoLeastDateAddDurationCollConditionCol))
                .select(cicliNdgColl.col("codicebanca_collegato"),
                        cicliNdgColl.col("ndg_collegato"),
                        cicliNdgColl.col("datainiziodef"),
                        cicliNdgColl.col("datafinedef"),
                        tlbuact.col("dt_riferimento").alias("datariferimento"),
                        tlbuact.col("tp_ndg").alias("natura_giuridica"),
                        tlbuact.col("intestazione"),
                        tlbuact.col("cd_fiscale").alias("codice_fiscale"),
                        tlbuact.col("partita_iva").alias("partitaiva"),
                        tlbuact.col("sae"), tlbuact.col("rae"), tlbuact.col("ciae"),
                        cicliNdgColl.col("provincia_segm").alias("provincia"),
                        tlbuact.col("provincia").alias("provincia_cod"),
                        tlbuact.col("sportello"),
                        cicliNdgColl.col("segmento"),
                        cicliNdgColl.col("cd_collegamento"),
                        tlbuact.col("ndg_caponucleo"),
                        cicliNdgColl.col("codicebanca"),
                        cicliNdgColl.col("ndgprincipale"));

        Dataset<Row> tlbcidefTlbuact = tlbcidefTlbuactPrinc.union(tlbcidefTlbuactColl).distinct();

        Dataset<Row> tlbudtc = sparkSession.read().format(csvFormat).option("delimiter", ",")
                .schema(fromPigSchemaToStructType(FanagMonthlySchema.getTlbudctPigSchema()))
                .csv(tlbudtcPath);

        // JOIN  tlbudtc BY (cd_istituto, ndg, dt_riferimento), tlbcidef_tlbuact BY (codicebanca_collegato,ndg_collegato, datariferimento) ;
        Column tlbudtcJoinCondition = tlbudtc.col("cd_istituto").equalTo(tlbcidefTlbuact.col("codicebanca_collegato"))
                .and(tlbudtc.col("ndg").equalTo(tlbcidefTlbuact.col("ndg_collegato")))
                .and(tlbudtc.col("dt_riferimento").equalTo(tlbcidefTlbuact.col("datariferimento")));

        List<Column> fanagOutSelectColList = new ArrayList<>(selectDfColumns(
                tlbcidefTlbuact, Arrays.asList("codicebanca_collegato", "ndg_collegato", "datariferimento")));

        fanagOutSelectColList.addAll(selectDfColumns(tlbudtc, Arrays.asList("totale_accordato", "totale_utilizzi", "tot_acco_mortgage",
                "tot_util_mortgage", "totale_saldi_0063", "totale_saldi_0260")));
        fanagOutSelectColList.addAll(selectDfColumns(tlbcidefTlbuact, Arrays.asList("natura_giuridica", "intestazione",
                "codice_fiscale", "partitaiva", "sae", "rae", "ciae", "provincia", "provincia_cod", "sportello")));
        fanagOutSelectColList.addAll(selectDfColumns(tlbudtc, Arrays.asList("posiz_soff_inc", "status_ndg", "tp_cli_scad_scf","cd_rap_ristr")));
        fanagOutSelectColList.addAll(selectDfColumns(tlbcidefTlbuact, Arrays.asList("segmento", "cd_collegamento", "ndg_caponucleo")));

        // (tlbudtc::tp_ristrutt != '0' ? 'S' : 'N') as flag_ristrutt
        Column flagRistruttCol = functions.when(tlbudtc.col("tp_ristrutt").notEqual(0), "S").otherwise("N");
        fanagOutSelectColList.add(flagRistruttCol);

        fanagOutSelectColList.addAll(selectDfColumns(tlbcidefTlbuact, Arrays.asList("codicebanca", "ndgprincipale", "datainiziodef")));

        Dataset<Row> fanagOut = tlbudtc.join(tlbcidefTlbuact, tlbudtcJoinCondition, "inner").select(toScalaColSeq(fanagOutSelectColList));

        fanagOut.write().format(csvFormat).option("delimiter", ",").mode(SaveMode.Overwrite).csv(Paths.get(stepOutputDir, fanagOutPath).toString());
    }
}
