package steps.schemas;

import java.util.LinkedHashMap;
import java.util.Map;

public class MovimentiSchema {

    public static Map<String, String> getTlbmovcontaPigSchema(){

        return new LinkedHashMap<String, String>(){{

            put("mo_dt_riferimento", "chararray");
            put("mo_istituto", "chararray");
            put("mo_ndg", "chararray");
            put("mo_sportello", "chararray");
            put("mo_conto", "chararray");
            put("mo_conto_esteso", "chararray");
            put("mo_num_soff", "chararray");
            put("mo_cat_rapp_soff", "chararray");
            put("mo_fil_rapp_soff", "chararray");
            put("mo_num_rapp_soff", "chararray");
            put("mo_id_movimento", "chararray");
            put("mo_categoria", "chararray");
            put("mo_causale", "chararray");
            put("mo_dt_contabile", "int");
            put("mo_dt_valuta", "chararray");
            put("mo_imp_movimento", "chararray");
            put("mo_flag_extracont", "chararray");
            put("mo_flag_storno", "chararray");
            put("mo_ndg_principale", "chararray");
            put("mo_dt_inizio_ciclo", "chararray");
        }};
    }
}
