package it.carloni.luca.lgd.schema;

import java.util.LinkedHashMap;
import java.util.Map;

public class SofferenzePreviewSchema {

    public static Map<String, String> getSoffLoadPigSchema(){

        return new LinkedHashMap<String, String>(){{

            put("istituto", "chararray");
            put("ndg", "chararray");
            put("numerosofferenza", "chararray");
            put("datainizio", "chararray");
            put("datafine", "chararray");
            put("statopratica", "chararray");
            put("saldoposizione", "chararray");
            put("saldoposizionecontab", "chararray");
        }};
    }
}
