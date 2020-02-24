package it.carloni.luca.lgd.schemas;

import java.util.LinkedHashMap;
import java.util.Map;

public class QuadFrappSchema {

    public static Map<String, String> getHadoopFrappPigSchema(){

        return new LinkedHashMap<String, String>(){{

            put("codicebanca", "chararray");
            put("ndg", "chararray");
            put("sportello", "chararray");
            put("conto", "chararray");
            put("datariferimento", "chararray");
            put("contoesteso", "chararray");
            put("formatecnica", "chararray");
            put("dataaccensione", "chararray");
            put("dataestinzione", "chararray");
            put("datascadenza", "chararray");
            put("r046tpammortam", "chararray");
            put("r071tprapporto", "chararray");
            put("r209periodliquid", "chararray");
            put("cdprodottorischio", "chararray");
            put("durataoriginaria", "chararray");
            put("r004divisa", "chararray");
            put("duarataresidua", "chararray");
            put("r239tpcontrrapp", "chararray");
            put("d2240marginefidorev", "chararray");
            put("d239marginefidoirrev", "chararray");
            put("d1788utilizzaccafirm", "chararray");
            put("d1780impaccordato", "chararray");
            put("r025flagristrutt", "chararray");
            put("d9322impaccorope", "chararray");
            put("d9323impaaccornonope", "chararray");
            put("d6464ggsconfino", "chararray");
            put("d0018fidonopesbfpi", "chararray");
            put("d0019fidopesbfpill", "chararray");
            put("d0623fidoopefirma", "chararray");
            put("d0009fidodpecassa", "chararray");
            put("d0008fidononopercas", "chararray");
            put("d0622fidononoperfir", "chararray");
            put("d0058fidonopesbfced", "chararray");
            put("d0059fidoopesbfced", "chararray");
            put("d0007autosconfcassa", "chararray");
            put("d0017autosconfsbf", "chararray");
            put("d0621autosconffirma", "chararray");
            put("d0967gamerci", "chararray");
            put("d0968garipoteca", "chararray");
            put("d0970garpersonale", "chararray");
            put("d0971garaltre", "chararray");
            put("d0985garobblpremiss", "chararray");
            put("d0986garobblbcentr", "chararray");
            put("d0987garobblaltre", "chararray");
            put("d0988gardepdenaro", "chararray");
            put("d0989garcdpremiss", "chararray");
            put("d0990garaltrivalori", "chararray");
            put("d0260valoredossier", "chararray");
            put("d0021fidoderiv", "chararray");
            put("d0061saldocont", "chararray");
            put("d0062aldocontdare", "chararray");
            put("d0063saldocontavere", "chararray");
            put("d0064partilliqsbf", "chararray");
            put("d0088indutilfidomed", "chararray");
            put("d0624impfidoaccfirma", "chararray");
            put("d0652imputilizzofirma", "chararray");
            put("d0982impgarperscred", "chararray");
            put("d1509impsallqmeddar", "chararray");
            put("d1785imputilizzocassa", "chararray");
            put("d0462impdervalintps", "chararray");
            put("d0475impdervalintng", "chararray");
            put("qcaprateascad", "chararray");
            put("qcaprateimpag", "chararray");
            put("qintrateimpag", "chararray");
            put("qcapratemora", "chararray");
            put("qintratemora", "chararray");
            put("accontiratescad", "chararray");
            put("imporigprestito", "chararray");
            put("d6998", "chararray");
            put("d6970", "chararray");
            put("d0075addebitiinsosp", "chararray");
            put("codicebanca_princ", "chararray");
            put("ndgprincipale", "chararray");
            put("datainiziodef", "chararray");
        }};
    }

    public static Map<String, String> getOldFrappLoadPigSchema(){

        return new LinkedHashMap<String, String>(){{

            put("CODICEBANCA", "chararray");
            put("NDG", "chararray");
            put("SPORTELLO", "chararray");
            put("CONTO", "chararray");
            put("PROGR_SEGMENTO", "chararray");
            put("DT_RIFERIMENTO", "chararray");
            put("CONTO_ESTESO", "chararray");
            put("FORMA_TECNICA", "chararray");
            put("DT_ACCENSIONE", "chararray");
            put("DT_ESTINZIONE", "chararray");
            put("DT_SCADENZA", "chararray");
            put("R046_TP_AMMORTAMENTO", "chararray");
            put("R071_TP_RAPPORTO", "chararray");
            put("R209_PERIOD_LIQUIDAZION", "chararray");
            put("CD_PRODOTTO_RISCHI", "chararray");
            put("DURATA_ORIGINARIA", "chararray");
            put("R004_DIVISA", "chararray");
            put("DURATA_RESIDUA", "chararray");
            put("R239_TP_CONTR_RAPP", "chararray");
            put("D2240_MARGINE_FIDO_REV", "chararray");
            put("D2239_MARGINE_FIDO_IRREV", "chararray");
            put("D1781_UTILIZZ_CASSA_FIRM", "chararray");
            put("D1780_IMP_ACCORDATO", "chararray");
            put("R025_FLAG_RISTRUTT", "chararray");
            put("D9322_IMP_ACCOR_OPE", "chararray");
            put("D9323_IMP_ACCOR_NON_OPE", "chararray");
            put("D6464_GG_SCONFINO", "chararray");
            put("D0018_FIDO_N_OPE_SBF_P_I", "chararray");
            put("D0019_FIDO_OPE_SBF_P_ILL", "chararray");
            put("D0623_FIDO_OPE_FIRMA", "chararray");
            put("D0009_FIDO_OPE_CASSA", "chararray");
            put("D0008_FIDO_NON_OPER_CAS", "chararray");
            put("D0622_FIDO_NON_OPER_FIR", "chararray");
            put("D0058_FIDO_N_OPE_SBF_CED", "chararray");
            put("D0059_FIDO_OPE_SBF_CED", "chararray");
            put("D0007_AUT_SCONF_CASSA", "chararray");
            put("D0017_AUT_SCONF_SBF", "chararray");
            put("D0621_AUT_SCONF_FIRMA", "chararray");
            put("D0967_GAR_MERCI", "chararray");
            put("D0968_GAR_IPOTECA", "chararray");
            put("D0970_GAR_PERSONALE", "chararray");
            put("D0971_GAR_ALTRE", "chararray");
            put("D0985_GAR_OBBL_PR_EMISS", "chararray");
            put("D0986_GAR_OBBL_B_CENTR", "chararray");
            put("D0987_GAR_OBBL_ALTRE", "chararray");
            put("D0988_GAR_DEP_DENARO", "chararray");
            put("D0989_GAR_CD_PR_EMISS", "chararray");
            put("D0990_GAR_ALTRI_VALORI", "chararray");
            put("D0260_VALORE_DOSSIER", "chararray");
            put("D0021_FIDO_DERIV", "chararray");
            put("D0061_SALDO_CONT", "chararray");
            put("D0062_SALDO_CONT_DARE", "chararray");
            put("D0063_SALDO_CONT_AVERE", "chararray");
            put("D0064_PART_ILLIQ_SBF", "chararray");
            put("D0088_IND_UTIL_FIDO_MED", "chararray");
            put("D0624_IMP_FIDO_ACC_FIRMA", "chararray");
            put("D0625_IMP_UTILIZZO_FIRMA", "chararray");
            put("D0982_IMP_GAR_PERS_CRED", "chararray");
            put("D1509_IMP_SAL_LQ_MED_DAR", "chararray");
            put("D1785_IMP_UTILIZZO_CASSA", "chararray");
            put("D0462_IMP_DER_VAL_INT_PS", "chararray");
            put("D0475_IMP_DER_VAL_INT_NG", "chararray");
            put("QCAPRATEASCAD", "chararray");
            put("QCAPRATEIMPAG", "chararray");
            put("QINTERRATEIMPAG", "chararray");
            put("QCAPRATEMORA", "chararray");
            put("QINTERRATEMORA", "chararray");
            put("ACCONTIRATESCAD", "chararray");
            put("IMPORIGPRESTITO", "chararray");
            put("CDFISC", "chararray");
            put("D6998_GAR_TITOLI", "chararray");
            put("D6970_GAR_PERS", "chararray");
            put("ADDEBITI_IN_SOSP", "chararray");
        }};
    }

    public static Map<String, String> getFcollPigSchema(){

        return new LinkedHashMap<String, String>(){{

            put("CODICEBANCA", "chararray");
            put("NDGPRINCIPALE", "chararray");
            put("DATAINIZIODEF", "chararray");
            put("DATAFINEDEF", "chararray");
            put("DATA_DEFAULT", "chararray");
            put("ISTITUTO_COLLEGATO", "chararray");
            put("NDG_COLLEGATO", "chararray");
            put("DATA_COLLEGAMENTO", "chararray");
            put("CUMULO", "chararray");
        }};
    }
}
