package it.carloni.luca.lgd.schemas;

import java.util.LinkedHashMap;
import java.util.Map;

public class QuadFcollCicliSchema {

    public static Map<String, String> getFcollPigSchema(){

        return new LinkedHashMap<String, String>(){{

                put("codicebanca", "chararray");
                put("ndgprincipale", "chararray");
                put("datainiziodef", "chararray");
                put("datafinedef", "chararray");
                put("data_default", "chararray");
                put("istituto_collegato", "chararray");
                put("ndg_collegato", "chararray");
                put("data_collegamento", "chararray");
                put("cumulo", "chararray");
        }};
    }

    public static Map<String, String> getCicliNdgLoadPigSchema(){

        return new LinkedHashMap<String, String>(){{

            put("cd_isti", "chararray");
            put("ndg_principale", "chararray");
            put("dt_inizio_ciclo", "chararray");
            put("dt_fine_ciclo", "chararray");
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
            put("ndg_coll", "chararray");
            put("cd_isti_coll", "chararray");
        }};
    }
}
