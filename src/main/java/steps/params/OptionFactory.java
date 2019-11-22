package steps.params;

import org.apache.commons.cli.Option;

public class OptionFactory {

    public static Option getDataDaOption(){

        return new Option("dd", "data-da", true, "parametro $data_da");
    }

    public static Option getDataAOpton(){

        return new Option("dd", "data-a", true, "parametro $data_a");
    }

    public static Option getUfficioOption(){

        return new Option("uf", "ufficio", true, "parametro $ufficio");
    }

    public static Option getNumeroMesi1Option(){

        return new Option("nm1", "numero-mesi-1", true, "parametro $numero_mesi_1");
    }

    public static Option getNumeroMesi2Option(){

        return new Option("nm2", "numero-mesi-2", true, "parametro $numero_mesi_2");
    }
}
