package it.carloni.luca.lgd.spark.step;

import it.carloni.luca.lgd.parameter.step.DataANumeroMesi12Value;
import it.carloni.luca.lgd.schema.FanagMonthlySchema;
import it.carloni.luca.lgd.spark.common.AbstractStep;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.log4j.Logger;
import org.apache.spark.sql.types.DataTypes;

import static it.carloni.luca.lgd.spark.utils.StepUtils.*;


public class FanagMonthly extends AbstractStep<DataANumeroMesi12Value> {

    private final Logger logger = Logger.getLogger(getClass());

    @Override
    public void run(DataANumeroMesi12Value dataANumeroMesi12Values) {

        logger.info(dataANumeroMesi12Values.toString());

        String cicliNdgPath = getValue("fanag.monthly.cicli.ndg.path.csv");
        String tlbuActPath = getValue("fanag.monthly.tlbuact.csv");
        String tlbudtcPath = getValue("fanag.monthly.tlbduct.path.csv");
        String fanagOutPath = getValue("fanag.monthly.fanag.out");

        logger.info("fanag.monthly.cicli.ndg.path.csv: " + cicliNdgPath);
        logger.info("fanag.monthly.tlbuact.csv: " + tlbuActPath);
        logger.info("fanag.monthly.tlbduct.path.csv: " + tlbudtcPath);
        logger.info("fanag.monthly.fanag.out: " + fanagOutPath);

        String dataA = dataANumeroMesi12Values.getDataA();
        int numeroMesi1 = dataANumeroMesi12Values.getNumeroMesi1();
        int numeroMesi2 = dataANumeroMesi12Values.getNumeroMesi2();
        String Y4M2D2Format = "yyyyMMdd";

        Dataset<Row> cicliNdg = readCsvAtPathUsingSchema(cicliNdgPath, FanagMonthlySchema.getCicliNdgPigSchema());

        // cicli_ndg_princ = FILTER cicli_ndg BY cd_collegamento IS NULL;
        // cicli_ndg_coll = FILTER cicli_ndg BY cd_collegamento IS NOT NULL;

        Dataset<Row> cicliNdgPrinc = cicliNdg.filter(cicliNdg.col("cd_collegamento").isNull());
        Dataset<Row> cicliNdgColl = cicliNdg.filter(cicliNdg.col("cd_collegamento").isNotNull());

        Dataset<Row> tlbuact = readCsvAtPathUsingSchema(tlbuActPath, FanagMonthlySchema.getTlbuactPigSchema())
                .selectExpr("dt_riferimento", "cd_istituto", "ndg", "tp_ndg", "intestazione",
                        "cd_fiscale", "partita_iva", "sae", "rae", "ciae", "provincia", "sportello", "ndg_caponucleo");

        // JOIN  tlbuact BY (cd_istituto, ndg), cicli_ndg_princ BY (codicebanca_collegato, ndg_collegato);
        Column tlbuactCicliNdgPrincJoinCondition = tlbuact.col("cd_istituto").equalTo(cicliNdgPrinc.col("codicebanca_collegato"))
                .and(tlbuact.col("ndg").equalTo(cicliNdgPrinc.col("ndg_collegato")));

        //  FILTER BY ToDate((chararray)dt_riferimento,'yyyyMMdd') >= SubtractDuration(ToDate((chararray)datainiziodef,'yyyyMMdd'),'$numero_mesi_1')
        Column dtRiferimentoDataInizioDefPrincConditionCol = toStringCol(tlbuact.col("dt_riferimento"))
                .geq(subtractDurationUDF(toStringCol(cicliNdgPrinc.col("datainiziodef")), Y4M2D2Format, numeroMesi1));

        // LeastDate( (int)ToString(SubtractDuration(ToDate((chararray)datafinedef,'yyyyMMdd' ),'P1M'),'yyyyMMdd'), $data_a )
        // [a] SubtractDuration(ToDate((chararray)datafinedef,'yyyyMMdd' ),'P1M'),'yyyyMMdd')
        Column cicliNdgPrincDataFineDefSubtractDurationCol = subtractDurationUDF(toStringCol(cicliNdgPrinc.col("datafinedef")), Y4M2D2Format, 1);

        // we need to format $data_a to pattern yyyyMMdd
        String dataAPattern = getValue("params.dataa.pattern");
        Column dataACol = functions.lit(changeDateFormat(dataA, dataAPattern, Y4M2D2Format));
        Column leastDatePrincCol = leastDateUDF(cicliNdgPrincDataFineDefSubtractDurationCol, dataACol, Y4M2D2Format);

        // AddDuration( ToDate( (chararray) leastDate(...),'yyyyMMdd'), $data_a ),'yyyyMMdd' ),'$numero_mesi_2' )
        Column leastDateAddDurationPrincCol = addDurationUDF(leastDatePrincCol, Y4M2D2Format, numeroMesi2);

        // SUBSTRING( (chararray)dt_riferimento,0,6 ) <= SUBSTRING(leastDateAddDurationPrincCol, 0,6)
        Column dtRiferimentoLeastDateAddDurationPrincConditionCol = substring06(tlbuact.col("dt_riferimento"))
                        .leq(substring06(leastDateAddDurationPrincCol));

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
                        tlbuact.col("tp_ndg").alias("naturagiuridica"),
                        tlbuact.col("intestazione"),
                        tlbuact.col("cd_fiscale").alias("codicefiscale"),
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
        Column dtRiferimentoDataInizioDefCollConditionCol = toStringCol(tlbuact.col("dt_riferimento"))
                .geq(subtractDurationUDF(toStringCol(cicliNdgColl.col("datainiziodef")),Y4M2D2Format, numeroMesi1));

        // LeastDate( (int)ToString(SubtractDuration(ToDate((chararray)datafinedef,'yyyyMMdd' ),'P1M'),'yyyyMMdd'), $data_a )
        Column cicliNdgCollDataFineDefSubtractDurationCol = subtractDurationUDF(toStringCol(cicliNdgColl.col("datafinedef")), Y4M2D2Format, 1);
        Column leastDateCollCol = leastDateUDF(cicliNdgCollDataFineDefSubtractDurationCol, dataACol, Y4M2D2Format);

        // AddDuration( ToDate( (chararray) leastDate(...),'yyyyMMdd'), $data_a ),'yyyyMMdd' ),'$numero_mesi_2' )
        Column leastDateAddDurationCollCol = addDurationUDF(leastDateCollCol, Y4M2D2Format, numeroMesi2);

        // SUBSTRING( (chararray)dt_riferimento,0,6 ) <= SUBSTRING(leastDateAddDurationPrincCol, 0,6)
        Column dtRiferimentoLeastDateAddDurationCollConditionCol = substring06(tlbuact.col("dt_riferimento"))
                        .leq(substring06(leastDateAddDurationCollCol));

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
                        tlbuact.col("tp_ndg").alias("naturagiuridica"),
                        tlbuact.col("intestazione"),
                        tlbuact.col("cd_fiscale").alias("codicefiscale"),
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

        Dataset<Row> tlbcidefTlbuact = tlbcidefTlbuactPrinc
                .union(tlbcidefTlbuactColl)
                .distinct();

        Dataset<Row> tlbudtc = readCsvAtPathUsingSchema(tlbudtcPath, FanagMonthlySchema.getTlbudctPigSchema());

        // JOIN  tlbudtc BY (cd_istituto, ndg, dt_riferimento), tlbcidef_tlbuact BY (codicebanca_collegato,ndg_collegato, datariferimento) ;
        Column tlbudtcJoinCondition = tlbudtc.col("cd_istituto").equalTo(tlbcidefTlbuact.col("codicebanca_collegato"))
                .and(tlbudtc.col("ndg").equalTo(tlbcidefTlbuact.col("ndg_collegato")))
                .and(tlbudtc.col("dt_riferimento").equalTo(tlbcidefTlbuact.col("datariferimento")));

        // (tlbudtc::tp_ristrutt != '0' ? 'S' : 'N') as flag_ristrutt
        Column flagRistruttCol = functions.when(tlbudtc.col("tp_ristrutt").notEqual("0"), "S").otherwise("N").as("flag_ristrutt");

        Dataset<Row> fanagOut = tlbudtc.join(tlbcidefTlbuact, tlbudtcJoinCondition, "inner")
                .select(tlbcidefTlbuact.col("codicebanca_collegato").as("codicebanca"),
                        tlbcidefTlbuact.col("ndg_collegato").as("ndg"), tlbcidefTlbuact.col("datariferimento"),
                        tlbudtc.col("totale_accordato").as("totaccordato"), tlbudtc.col("totale_utilizzi").as("totutilizzo"),
                        tlbudtc.col("tot_acco_mortgage").as("totaccomortgage"), tlbudtc.col("tot_util_mortgage").as("totutilmortgage"),
                        tlbudtc.col("totale_saldi_0063").as("totsaldi0063"), tlbudtc.col("totale_saldi_0260").as("totsaldi0260"),
                        tlbcidefTlbuact.col("naturagiuridica"), tlbcidefTlbuact.col("intestazione"),
                        tlbcidefTlbuact.col("codicefiscale"), tlbcidefTlbuact.col("partitaiva"),
                        tlbcidefTlbuact.col("sae"), tlbcidefTlbuact.col("rae"), tlbcidefTlbuact.col("ciae"),
                        tlbcidefTlbuact.col("provincia"), tlbcidefTlbuact.col("provincia_cod"),
                        tlbcidefTlbuact.col("sportello"), tlbudtc.col("posiz_soff_inc").as("attrn011"),
                        tlbudtc.col("status_ndg").as("attrn175"), tlbudtc.col("tp_cli_scad_scf").as("attrn186"),
                        tlbudtc.col("cd_rap_ristr").as("cdrapristr"), tlbcidefTlbuact.col("segmento"),
                        tlbcidefTlbuact.col("cd_collegamento"), tlbcidefTlbuact.col("ndg_caponucleo"), flagRistruttCol,
                        tlbcidefTlbuact.col("codicebanca").as("codicebanca_princ"),
                        tlbcidefTlbuact.col("ndgprincipale").as("ndgprincipale"),
                        tlbcidefTlbuact.col("datainiziodef").as("datainiziodef"));

        writeDatasetAsCsvAtPath(fanagOut, fanagOutPath);
    }

    private Column substring06(Column column) {

        return functions.substring(column.cast(DataTypes.StringType), 0, 6);
    }
}

