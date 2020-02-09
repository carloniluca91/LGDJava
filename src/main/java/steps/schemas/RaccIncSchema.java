package steps.schemas;

import java.util.LinkedHashMap;
import java.util.Map;

public class RaccIncSchema {

    public static Map<String, String> getTlbmignPigSchema(){

        return new LinkedHashMap<String, String>(){{

            put("cd_isti_ced", "chararray");
            put("ndg_ced", "chararray");
            put("cd_abi_ced", "chararray");
            put("cd_isti_ric", "chararray");
            put("ndg_ric", "chararray");
            put("cd_abi_ric", "chararray");
            put("fl01_anag", "chararray");
            put("fl02_anag", "chararray");
            put("fl03_anag", "chararray");
            put("fl04_anag", "chararray");
            put("fl05_anag", "chararray");
            put("fl06_anag", "chararray");
            put("fl07_anag", "chararray");
            put("fl08_anag", "chararray");
            put("fl09_anag", "chararray");
            put("fl10_anag", "chararray");
            put("cod_migraz", "chararray");
            put("data_migraz", "chararray");
            put("data_ini_appl", "chararray");
            put("data_fin_appl", "chararray");
            put("data_ini_appl2", "chararray");
        }};
    }
}
