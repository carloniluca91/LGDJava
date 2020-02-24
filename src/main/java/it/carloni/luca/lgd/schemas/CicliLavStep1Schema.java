package it.carloni.luca.lgd.schemas;

import java.util.LinkedHashMap;
import java.util.Map;

public class CicliLavStep1Schema {

    public static Map<String, String> getTlbcidefPigSchema(){

        return new LinkedHashMap<String, String>() {{
            put("cd_isti", "chararray");
            put("ndg_principale", "chararray");
            put("cod_cr", "chararray");
            put("dt_inizio_ciclo", "int");
            put("dt_ingresso_status", "int");
            put("status_ingresso", "chararray");
            put("dt_uscita_status", "chararray");
            put("status_uscita", "chararray");
            put("dt_fine_ciclo", "chararray");
            put("indi_pastdue", "chararray");
            put("indi_impr_priv", "chararray"); }};
    }

    public static Map<String, String> getTlbcraccLoadPigSchema() {

        return new LinkedHashMap<String, String>() {{
            put("data_rif", "int");
            put("cd_isti", "chararray");
            put("ndg", "chararray");
            put("cod_raccordo", "chararray");
            put("data_val", "int"); }};
    }
}
