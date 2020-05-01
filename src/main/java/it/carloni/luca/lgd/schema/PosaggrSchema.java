package it.carloni.luca.lgd.schema;

import java.util.LinkedHashMap;
import java.util.Map;

public class PosaggrSchema {

    public static Map<String, String> getTblCompPigSchema(){

        return new LinkedHashMap<String, String>(){{

            put("dt_riferimento", "int");
            put("c_key", "chararray");
            put("tipo_segmne", "chararray");
            put("cd_istituto", "chararray");
            put("ndg", "chararray");
        }};
    }

    public static Map<String, String> getTlbaggrPigSchema(){

        return new LinkedHashMap<String, String>(){{

            put("dt_riferimento", "int");
            put("c_key_aggr", "chararray");
            put("ndg_gruppo", "chararray");
            put("cod_fiscale", "chararray");
            put("tipo_segmne_aggr", "chararray");
            put("segmento", "chararray");
            put("tipo_motore", "chararray");
            put("cd_istituto", "chararray");
            put("ndg", "chararray");
            put("rae", "chararray");
            put("sae", "chararray");
            put("tp_ndg", "chararray");
            put("prov_segm", "chararray");
            put("fonte_segmento", "chararray");
            put("utilizzo_cr", "chararray");
            put("accordato_cr", "chararray");
            put("databil", "chararray");
            put("strutbil", "chararray");
            put("fatturbil", "chararray");
            put("attivobil", "chararray");
            put("codimp_cebi", "chararray");
            put("tot_acco_agr", "double");
            put("tot_util_agr", "chararray");
            put("n058_int_vig", "chararray");
        }};
    }

    public static Map<String, String> getTlbposiLoadPigSchema(){

        return new LinkedHashMap<String, String>(){{

            put("dt_riferimento", "int");
            put("cd_istituto", "chararray");
            put("ndg", "chararray");
            put("c_key", "chararray");
            put("cod_fiscale", "chararray");
            put("ndg_gruppo", "chararray");
            put("bo_acco", "chararray");
            put("bo_util", "chararray");
            put("tot_add_sosp", "chararray");
            put("tot_val_intr", "chararray");
            put("ca_acco", "chararray");
            put("ca_util", "chararray");
            put("fl_incaglio", "chararray");
            put("fl_soff", "chararray");
            put("fl_inc_ogg", "chararray");
            put("fl_ristr", "chararray");
            put("fl_pd_90", "chararray");
            put("fl_pd_180", "chararray");
            put("util_cassa", "chararray");
            put("fido_op_cassa", "chararray");
            put("utilizzo_titoli", "chararray");
            put("esposizione_titoli", "chararray");
        }};
    }
}
