package steps.schemas;

import java.util.LinkedHashMap;
import java.util.Map;

public class FpasperdSchema {

    public static Map<String, String> getTlbcidefLoadPigSchema(){

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

        }};
    }

    public static Map<String, String> getTlbpaspeFilterPigSchema(){

        return new LinkedHashMap<String, String>() {{

            put("cd_istituto", "chararray");
            put("ndg", "chararray");
            put("datacont", "int");
            put("causale", "chararray");
            put("importo", "chararray");

        }};
    }

    public static Map<String, String> getTlbpaspeossPigSchema(){

        return new LinkedHashMap<String, String>(){{

            put("cd_istituto", "chararray");
            put("ndg", "chararray");
            put("datacont", "int");
            put("causale", "chararray");
            put("importo", "chararray");

        }};
    }
}
