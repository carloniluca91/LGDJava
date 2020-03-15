package it.carloni.luca.lgd.option;

import org.apache.commons.cli.Option;

public class OptionFactory {

    private static Option createOption(String shortOpt, String longOpt, String description){

        return new Option(shortOpt, longOpt, true, description);
    }

    public static Option getDataAOpton(){

        return createOption(OptionEnum.DATA_A_SHORT_OPTION,
                OptionEnum.DATA_A_LONG_OPTION,
                OptionEnum.DATA_A_OPTION_DESCRIPTION);
    }

    public static Option getDataDaOption(){

        return createOption(OptionEnum.DATA_DA_SHORT_OPTION,
                OptionEnum.DATA_DA_LONG_OPTION,
                OptionEnum.DATA_DA_OPTION_DESCRIPTION);
    }

    public static Option getDataOsservazioneOption(){

        return createOption(OptionEnum.DATA_OSSERVAZIONE_SHORT_OPTION,
                OptionEnum.DATA_OSSERVAZIONE_LONG_OPTION,
                OptionEnum.DATA_OSSERVAZIONE_OPTION_DESCRIPTION);
    }

    public static Option getUfficioOption(){

        return createOption(OptionEnum.UFFICIO_SHORT_OPTION,
                OptionEnum.UFFICIO_LONG_OPTION,
                OptionEnum.UFFICIO_OPTION_DESCRIPTION);
    }

    public static Option getNumeroMesi1Option(){

        return createOption(OptionEnum.NUMERO_MESI_1_SHORT_OPTION,
                OptionEnum.NUMERO_MESI_1_LONG_OPTION,
                OptionEnum.NUMERO_MESI_1_OPTION_DESCRIPTION);
    }

    public static Option getNumeroMesi2Option(){

        return createOption(OptionEnum.NUMERO_MESI_2_SHORT_OPTION,
                OptionEnum.NUMERO_MESI_2_LONG_OPTION,
                OptionEnum.NUMERO_MESI_2_OPTION_DESCRIPTION);
    }
}
