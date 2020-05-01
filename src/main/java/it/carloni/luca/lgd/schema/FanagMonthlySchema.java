package it.carloni.luca.lgd.schema;

import java.util.LinkedHashMap;
import java.util.Map;

public class FanagMonthlySchema {

    public static Map<String, String> getCicliNdgPigSchema(){

        return new LinkedHashMap<String, String>() {{

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

    public static Map<String, String> getTlbuactPigSchema(){

        return new LinkedHashMap<String, String>(){{

            put("cd_istituto", "chararray");
            put("ndg", "chararray");
            put("dt_riferimento", "int");
            put("cab_prov_com_stato", "chararray");
            put("provincia", "chararray");
            put("sae", "chararray");
            put("rae", "chararray");
            put("ciae", "chararray");
            put("sportello", "chararray");
            put("area_affari", "chararray");
            put("tp_ndg", "chararray");
            put("intestazione", "chararray");
            put("partita_iva", "chararray");
            put("cd_fiscale", "chararray");
            put("ctp_cred_proc_conc", "chararray");
            put("cliente_protestato", "chararray");
            put("status_sygei", "chararray");
            put("ndg_caponucleo", "chararray");
            put("specie_giuridica", "chararray");
            put("dt_inizio_rischio", "chararray");
            put("dt_revoca_fidi", "chararray");
            put("dt_cens_anagrafe", "chararray");
            put("cab_luogo_nascita", "chararray");
            put("dt_costituz_nascit", "chararray");
            put("tp_contr_basilea1", "chararray");
            put("flag_fallibile", "chararray");
            put("stato_controparte", "chararray");
            put("dt_estinz_ctp", "chararray");
            put("cliente_fido_rev", "chararray");
            put("tp_controparte", "chararray");
            put("grande_sett_attiv", "chararray");
            put("grande_ramo_attiv", "chararray");
            put("n058_interm_vigil", "chararray");
            put("n160_cod_ateco", "chararray");
        }};
    }

    public static Map<String, String> getTlbudctPigSchema(){

        return new LinkedHashMap<String, String>(){{

            put("cd_istituto", "chararray");
            put("ndg", "chararray");
            put("dt_riferimento", "int");
            put("reddito", "chararray");
            put("totale_utilizzi", "chararray");
            put("totale_fidi_delib", "chararray");
            put("totale_accordato", "chararray");
            put("patrimonio", "chararray");
            put("n_dipendenti", "chararray");
            put("tot_rischi_indir", "chararray");
            put("status_ndg", "chararray");
            put("status_basilea2", "chararray");
            put("posiz_soff_inc", "chararray");
            put("dubb_esito_inc_ndg", "chararray");
            put("status_cliente_lab", "chararray");
            put("tot_util_mortgage", "chararray");
            put("tot_acco_mortgage", "chararray");
            put("dt_entrata_default", "chararray");
            put("cd_stato_def_t0", "chararray");
            put("cd_tipo_def_t0", "chararray");
            put("cd_stato_def_a_t12", "chararray");
            put("cd_rap_ristr", "chararray");
            put("tp_ristrutt", "chararray");
            put("tp_cli_scad_scf", "chararray");
            put("tp_cli_scad_scf_b2", "chararray");
            put("totale_saldi_0063", "chararray");
            put("totale_saldi_0260", "chararray");
        }};
    }
}
