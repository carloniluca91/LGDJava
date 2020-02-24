package it.carloni.luca.lgd.schemas;

import java.util.LinkedHashMap;
import java.util.Map;

public class FrappPumaSchema {

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

    public static Map<String, String> getTlbgaranPigSchema(){

        return new LinkedHashMap<String, String>(){{

            put("cd_istituto", "chararray");
            put("ndg", "chararray");
            put("sportello", "chararray");
            put("dt_riferimento", "int");
            put("conto_esteso", "chararray");
            put("cd_puma2", "chararray");
            put("ide_garanzia", "chararray");
            put("importo", "chararray");
            put("fair_value", "chararray");
        }};
    }
}
