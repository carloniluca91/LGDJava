package steps.lgdstep;

import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import steps.abstractstep.AbstractStep;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

public class QuadFrapp extends AbstractStep {

    private String ufficio;

    public QuadFrapp(String loggerName, String ufficio){

        super(loggerName);
        logger = Logger.getLogger(loggerName);

        this.ufficio = ufficio;

        stepInputDir = getLGDPropertyValue("quad.frapp.input.dir");
        stepOutputDir = getLGDPropertyValue("quad.frapp.output.dir");

        logger.debug("stepInputDir: " + stepInputDir);
        logger.debug("stepOutputDir: " + stepOutputDir);
    }

    @Override
    public void run() {

        String csvFormat = getLGDPropertyValue("csv.format");
        String hadoopFrappCsv = getLGDPropertyValue("hadoop.frapp.csv");
        String oldFrappLoadCsv = getLGDPropertyValue("old.frapp.load.csv");
        String fcollCsv = getLGDPropertyValue("fcoll.csv");

        logger.debug("csvFormat: " + csvFormat);
        logger.debug("hadoopFrappCsv: " + hadoopFrappCsv);
        logger.debug("oldFrappLoadCsv: " + oldFrappLoadCsv);
        logger.debug("fcollCsv: " + fcollCsv);

        List<String> hadoopFrappColumnNames = Arrays.asList("codicebanca", "ndg", "sportello", "conto", "datariferimento", "contoesteso",
                "formatecnica", "dataaccensione", "dataestinzione", "datascadenza", "r046tpammortam", "r071tprapporto", "r209periodliquid",
                "cdprodottorischio", "durataoriginaria", "r004divisa", "duarataresidua", "r239tpcontrrapp", "d2240marginefidorev",
                "d239marginefidoirrev", "d1788utilizzaccafirm", "d1780impaccordato", "r025flagristrutt", "d9322impaccorope",
                "d9323impaaccornonope", "d6464ggsconfino", "d0018fidonopesbfpi", "d0019fidopesbfpill", "d0623fidoopefirma",
                "d0009fidodpecassa", "d0008fidononopercas", "d0622fidononoperfir", "d0058fidonopesbfced", "d0059fidoopesbfced",
                "d0007autosconfcassa", "d0017autosconfsbf", "d0621autosconffirma", "d0967gamerci", "d0968garipoteca",
                "d0970garpersonale", "d0971garaltre", "d0985garobblpremiss", "d0986garobblbcentr", "d0987garobblaltre",
                "d0988gardepdenaro", "d0989garcdpremiss", "d0990garaltrivalori", "d0260valoredossier", "d0021fidoderiv", "d0061saldocont",
                "d0062aldocontdare", "d0063saldocontavere", "d0064partilliqsbf", "d0088indutilfidomed", "d0624impfidoaccfirma",
                "d0652imputilizzofirma", "d0982impgarperscred", "d1509impsallqmeddar", "d1785imputilizzocassa", "d0462impdervalintps",
                "d0475impdervalintng", "qcaprateascad", "qcaprateimpag", "qintrateimpag", "qcapratemora", "qintratemora", "accontiratescad",
                "imporigprestito", "d6998", "d6970", "d0075addebitiinsosp", "codicebanca_princ", "ndgprincipale", "datainiziodef");

        Dataset<Row> hadoopFrapp = sparkSession.read().format(csvFormat).option("sep", ",")
                .schema(getStringTypeSchema(hadoopFrappColumnNames))
                .csv(Paths.get(stepInputDir, hadoopFrappCsv).toString());

        List<String> oldFrappLoadColumnNames = Arrays.asList("CODICEBANCA", "NDG", "SPORTELLO", "CONTO", "PROGR_SEGMENTO", "DT_RIFERIMENTO",
                "CONTO_ESTESO", "FORMA_TECNICA", "DT_ACCENSIONE", "DT_ESTINZIONE", "DT_SCADENZA", "R046_TP_AMMORTAMENTO", "R071_TP_RAPPORTO",
                "R209_PERIOD_LIQUIDAZION", "CD_PRODOTTO_RISCHI", "DURATA_ORIGINARIA", "R004_DIVISA", "DURATA_RESIDUA", "R239_TP_CONTR_RAPP",
                "D2240_MARGINE_FIDO_REV", "D2239_MARGINE_FIDO_IRREV", "D1781_UTILIZZ_CASSA_FIRM", "D1780_IMP_ACCORDATO", "R025_FLAG_RISTRUTT",
                "D9322_IMP_ACCOR_OPE", "D9323_IMP_ACCOR_NON_OPE", "D6464_GG_SCONFINO", "D0018_FIDO_N_OPE_SBF_P_I", "D0019_FIDO_OPE_SBF_P_ILL",
                "D0623_FIDO_OPE_FIRMA", "D0009_FIDO_OPE_CASSA", "D0008_FIDO_NON_OPER_CAS", "D0622_FIDO_NON_OPER_FIR", "D0058_FIDO_N_OPE_SBF_CED",
                "D0059_FIDO_OPE_SBF_CED", "D0007_AUT_SCONF_CASSA", "D0017_AUT_SCONF_SBF", "D0621_AUT_SCONF_FIRMA", "D0967_GAR_MERCI",
                "D0968_GAR_IPOTECA", "D0970_GAR_PERSONALE", "D0971_GAR_ALTRE", "D0985_GAR_OBBL_PR_EMISS", "D0986_GAR_OBBL_B_CENTR",
                "D0987_GAR_OBBL_ALTRE", "D0988_GAR_DEP_DENARO", "D0989_GAR_CD_PR_EMISS", "D0990_GAR_ALTRI_VALORI", "D0260_VALORE_DOSSIER",
                "D0021_FIDO_DERIV", "D0061_SALDO_CONT", "D0062_SALDO_CONT_DARE", "D0063_SALDO_CONT_AVERE", "D0064_PART_ILLIQ_SBF",
                "D0088_IND_UTIL_FIDO_MED", "D0624_IMP_FIDO_ACC_FIRMA", "D0625_IMP_UTILIZZO_FIRMA", "D0982_IMP_GAR_PERS_CRED",
                "D1509_IMP_SAL_LQ_MED_DAR", "D1785_IMP_UTILIZZO_CASSA", "D0462_IMP_DER_VAL_INT_PS", "D0475_IMP_DER_VAL_INT_NG",
                "QCAPRATEASCAD", "QCAPRATEIMPAG", "QINTERRATEIMPAG", "QCAPRATEMORA", "QINTERRATEMORA", "ACCONTIRATESCAD", "IMPORIGPRESTITO",
                "CDFISC", "D6998_GAR_TITOLI", "D6970_GAR_PERS", "ADDEBITI_IN_SOSP");

        Dataset<Row> oldFrappLoad = sparkSession.read().format(csvFormat).option("sep", ",")
                .schema(getStringTypeSchema(oldFrappLoadColumnNames))
                .csv(Paths.get(stepInputDir, oldFrappLoadCsv).toString());

        List<String> fcollColumnNames = Arrays.asList("CODICEBANCA", "NDGPRINCIPALE", "DATAINIZIODEF", "DATAFINEDEF",
                "DATA_DEFAULT", "ISTITUTO_COLLEGATO", "NDG_COLLEGATO", "DATA_COLLEGAMENTO", "CUMULO");

        Dataset<Row> fcoll = sparkSession.read().format(csvFormat).option("sep", ",")
                .schema(getStringTypeSchema(fcollColumnNames))
                .csv(Paths.get(stepInputDir, fcollCsv).toString());

        // JOIN oldfrapp_load BY (CODICEBANCA, NDG), fcoll BY (ISTITUTO_COLLEGATO, NDG_COLLEGATO);

        Column joinCondition = oldFrappLoad.col("CODICEBANCA").equalTo(fcoll.col("ISTITUTO_COLLEGATO"))
                .and(oldFrappLoad.col("NDG").equalTo(fcoll.col("NDG_COLLEGATO")));

        // FILTER
        // BY ToDate(oldfrapp_load::DT_RIFERIMENTO,'yyyyMMdd') >= ToDate( fcoll::DATAINIZIODEF,'yyyyMMdd')
        // AND ToDate(oldfrapp_load::DT_RIFERIMENTO,'yyyyMMdd') <= ToDate( fcoll::DATAFINEDEF,'yyyyMMdd'  )

        Column filterCondition = dateBetween(oldFrappLoad.col("DT_RIFERIMENTO"), "yyyyMMdd",
                fcoll.col("DATAINIZIODEF"), "yyyyMMdd",
                fcoll.col("DATAFINEDEF"), "yyyyMMdd");

        Dataset<Row> oldFrapp = oldFrappLoad.join(fcoll, joinCondition).filter(filterCondition).select(oldFrappLoad.col("*"),
                fcoll.col("CODICEBANCA").alias("CODICEBANCA_PRINC"), fcoll.col("NDGPRINCIPALE"), fcoll.col("DATAINIZIODEF"));

        // JOIN hadoop_frapp BY (codicebanca_princ, ndgprincipale, datainiziodef, codicebanca, ndg, sportello, conto, datariferimento) FULL OUTER,
        // oldfrapp BY (CODICEBANCA_PRINC, NDGPRINCIPALE, DATAINIZIODEF, CODICEBANCA, NDG, SPORTELLO, CONTO, DT_RIFERIMENTO);

        List<String> joinColumnNames = Arrays.asList("codicebanca_princ",
                "ndgprincipale", "datainiziodef", "codicebanca", "ndg", "sportello", "conto");
        joinCondition = getQuadJoinCondition(hadoopFrapp, oldFrapp, joinColumnNames)
                .and(hadoopFrapp.col("datariferimento").equalTo(oldFrapp.col("DT_RIFERIMENTO")));

        // FILTER hadoop_frapp_oldfrapp_join BY oldfrapp::CODICEBANCA IS NULL;

        Dataset<Row> hadoopFrappOut = hadoopFrapp.join(oldFrapp, joinCondition, "full_outer")
                .filter(oldFrapp.col("CODICEBANCA").isNull())
                .select(functions.lit(ufficio).alias("ufficio"), hadoopFrapp.col("*"), oldFrapp.col("*"));

        Dataset<Row> oldFrappOut = hadoopFrapp.join(oldFrapp, joinCondition, "full_outer")
                .filter(hadoopFrapp.col("codicebanca").isNull())
                .select(functions.lit(ufficio).alias("ufficio"), hadoopFrapp.col("*"), oldFrapp.col("*"));

        String hadoopFrappOutPath = getLGDPropertyValue("hadoop.frapp.out");
        String oldFrappOutPath = getLGDPropertyValue("old.frapp.out");

        logger.debug("hadoopFrappOutPath: " + hadoopFrappOutPath);
        logger.debug("oldFrappOutPath: " + oldFrappOutPath);

        hadoopFrappOut.write().format(csvFormat).option("sep", ",").mode(SaveMode.Overwrite)
                .csv(Paths.get(stepOutputDir, hadoopFrappOutPath).toString());

        oldFrappOut.write().format(csvFormat).option("sep", ",").mode(SaveMode.Overwrite)
                .csv(Paths.get(stepOutputDir, oldFrappOutPath).toString());
    }
}
