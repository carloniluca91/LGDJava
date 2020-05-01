package it.carloni.luca.lgd.schema;

import java.util.LinkedHashMap;
import java.util.Map;

public class FrappNdgMonthlySchema {

    public static Map<String, String> getTlbcidefPigSchema(){

        return new LinkedHashMap<String, String>(){{

            put("codicebanca", "chararray");
            put("ndgprincipale", "chararray");
            put("datainiziodef", "int");
            put("datafinedef", "int");
            put("datainiziopd", "chararray");
            put("datainizioristrutt", "chararray");
            put("datainizioinc", "chararray");
            put("datainiziosoff", "chararray");
            put("c_key", "chararray");
            put("tipo_segmne", "chararray");
            put("sae_segm", "chararray");
            put("rae_segm", "chararray");
            put("segmento", "chararray");
            put("tp_ndg", "chararray");
            put("provincia_segm", "chararray");
            put("databilseg", "chararray");
            put("strbilseg", "chararray");
            put("attivobilseg", "chararray");
            put("fatturbilseg", "chararray");
            put("ndg_collegato", "chararray");
            put("codicebanca_collegato", "chararray");
            put("cd_collegamento", "chararray");
            put("cd_fiscale", "chararray");
            put("dt_rif_udct", "int");
        }};
    }

    public static Map<String, String> getTlburttPigSchema(){

        return new LinkedHashMap<String, String>(){{

            put("cd_istituto", "chararray");
            put("ndg", "chararray");
            put("sportello", "chararray");
            put("conto", "chararray");
            put("progr_segmento", "int");
            put("dt_riferimento", "int");
            put("conto_esteso", "chararray");
            put("forma_tecnica", "chararray");
            put("flag_durata_contr", "chararray");
            put("cred_agevolato", "chararray");
            put("operazione_pool", "chararray");
            put("dt_accensione", "chararray");
            put("dt_estinzione", "chararray");
            put("dt_scadenza", "chararray");
            put("organo_deliber", "chararray");
            put("dt_delibera", "chararray");
            put("dt_scad_fido", "chararray");
            put("origine_rapporto", "chararray");
            put("tp_ammortamento", "chararray");
            put("tp_rapporto", "chararray");
            put("period_liquid", "chararray");
            put("freq_remargining", "chararray");
            put("cd_prodotto_ris", "chararray");
            put("durata_originaria", "chararray");
            put("divisa", "chararray");
            put("score_erogaz", "chararray");
            put("durata_residua", "chararray");
            put("categoria_sof", "chararray");
            put("categoria_inc", "chararray");
            put("dur_res_default", "chararray");
            put("flag_margine", "chararray");
            put("dt_entrata_def", "chararray");
            put("tp_contr_rapp", "chararray");
            put("cd_eplus", "chararray");
            put("r792_tipocartol", "chararray");
        }};
    }
}
