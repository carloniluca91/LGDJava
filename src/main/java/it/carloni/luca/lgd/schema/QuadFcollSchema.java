package it.carloni.luca.lgd.schema;

import java.util.LinkedHashMap;
import java.util.Map;

public class QuadFcollSchema {

    public static Map<String, String> getFcollLoadPigSchema(){

        return new LinkedHashMap<String, String>(){{

            put("cumulo", "chararray");
            put("cd_istituto_COLL", "chararray");
            put("ndg_COLL", "chararray");
            put("data_inizio_DEF", "chararray");
            put("data_collegamento", "chararray");
            put("pri", "chararray");
        }};
    }

    public static Map<String, String> getOldFposiLoadPigSchema(){

        return new LinkedHashMap<String, String>(){{

            put("datainizioDEF", "chararray");
            put("dataFINEDEF", "chararray");
            put("dataINIZIOPD", "chararray");
            put("datainizioinc", "chararray");
            put("dataSOFFERENZA", "chararray");
            put("codicebanca", "chararray");
            put("ndgprincipale", "chararray");
            put("flagincristrut", "chararray");
            put("cumulo", "chararray");
        }};
    }
}
