package it.carloni.luca.lgd.schema;

import java.util.LinkedHashMap;
import java.util.Map;

public class QuadFposiSchema {

    public static Map<String, String> getHadoopFposiPigSchema(){

        return new LinkedHashMap<String, String>(){{

            put("codicebanca", "chararray");
            put("ndgprincipale", "chararray");
            put("datainiziodef", "chararray");
            put("datafinedef", "chararray");
            put("ndg_gruppo", "chararray");
            put("datainiziopd", "chararray");
            put("datainizioinc", "chararray");
            put("datainizioristrutt", "chararray");
            put("datainiziosoff", "chararray");
            put("totaccordatodatdef", "chararray");
            put("totutilizzdatdef", "chararray");
            put("naturagiuridica_segm", "chararray");
            put("intestazione", "chararray");
            put("codicefiscale_segm", "chararray");
            put("partitaiva_segm", "chararray");
            put("sae_segm", "chararray");
            put("rae_segm", "chararray");
            put("ciae_ndg", "chararray");
            put("provincia_segm", "chararray");
            put("ateco", "chararray");
            put("segmento", "chararray");
            put("databilseg", "chararray");
            put("strbilseg", "chararray");
            put("attivobilseg", "chararray");
            put("fatturbilseg", "chararray");
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
