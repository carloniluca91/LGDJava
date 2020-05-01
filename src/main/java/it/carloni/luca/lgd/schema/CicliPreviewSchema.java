package it.carloni.luca.lgd.schema;

import java.util.LinkedHashMap;
import java.util.Map;

public class CicliPreviewSchema {

    public static Map<String, String> getFposiOutDirPigSchema(){

        return new LinkedHashMap<String, String>(){{

            put("codicebanca", "chararray");
            put("ndgprincipale", "chararray");
            put("datainiziodef", "chararray");
            put("datafinedef", "chararray");
            put("datainiziopd", "chararray");
            put("datainizioinc", "chararray");
            put("datainizioristrutt", "chararray");
            put("datasofferenza", "chararray");
            put("totaccordatodatdef", "double");
            put("totutilizzdatdef", "double");
            put("segmento", "chararray");
            put("naturagiuridica_segm", "chararray");

        }};
    }
}
