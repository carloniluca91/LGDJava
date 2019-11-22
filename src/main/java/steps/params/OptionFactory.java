package steps.params;

import org.apache.commons.cli.Option;

public class OptionFactory {

    private static Option createOption(String shortOpt, String longOpt, String description){

        return new Option(shortOpt, longOpt, true, description);
    }

    public static Option getDataAOpton(){

        return createOption("dd", "data-a", "parametro $data_a");
    }

    public static Option getDataDaOption(){

        return createOption("dd", "data-da", "parametro $data_da");
    }

    public static Option getDataOsservazioneOption(){

        return createOption("dao", "data-osservazione", "parametro $data_osservazione");
    }

    public static Option getUfficioOption(){

        return createOption("uf", "ufficio", "parametro $ufficio");
    }

    public static Option getNumeroMesi1Option(){

        return createOption("nm1", "numero-mesi-1", "parametro $numero_mesi_1");
    }

    public static Option getNumeroMesi2Option(){

        return createOption("nm2", "numero-mesi-2", "parametro $numero_mesi_2");
    }
}
