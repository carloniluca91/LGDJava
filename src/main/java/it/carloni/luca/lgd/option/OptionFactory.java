package it.carloni.luca.lgd.option;

import org.apache.commons.cli.Option;

public class OptionFactory {

    private static Option getRequiredOptionWithArg(String shortOpt, String longOpt, String description){

        Option newOption = new Option(shortOpt, longOpt, true, description);
        newOption.setRequired(true);
        return newOption;
    }

    public static Option getStepNameOption() {

        return getRequiredOptionWithArg(OptionEnum.STEP_NAME_SHORT_OPTION.getString(),
                OptionEnum.STEP_NAME_LONG_OPTION.getString(),
                OptionEnum.STEP_NAME_OPTION_DESCRIPTION.getString());
    }

    public static Option getDataAOpton(){

        return getRequiredOptionWithArg(OptionEnum.DATA_A_SHORT_OPTION.getString(),
                OptionEnum.DATA_A_LONG_OPTION.getString(),
                OptionEnum.DATA_A_OPTION_DESCRIPTION.getString());
    }

    public static Option getDataDaOption(){

        return getRequiredOptionWithArg(OptionEnum.DATA_DA_SHORT_OPTION.getString(),
                OptionEnum.DATA_DA_LONG_OPTION.getString(),
                OptionEnum.DATA_DA_OPTION_DESCRIPTION.getString());
    }

    public static Option getDataOsservazioneOption(){

        return getRequiredOptionWithArg(OptionEnum.DATA_OSSERVAZIONE_SHORT_OPTION.getString(),
                OptionEnum.DATA_OSSERVAZIONE_LONG_OPTION.getString(),
                OptionEnum.DATA_OSSERVAZIONE_OPTION_DESCRIPTION.getString());
    }

    public static Option getUfficioOption(){

        return getRequiredOptionWithArg(OptionEnum.UFFICIO_SHORT_OPTION.getString(),
                OptionEnum.UFFICIO_LONG_OPTION.getString(),
                OptionEnum.UFFICIO_OPTION_DESCRIPTION.getString());
    }

    public static Option getNumeroMesi1Option(){

        return getRequiredOptionWithArg(OptionEnum.NUMERO_MESI_1_SHORT_OPTION.getString(),
                OptionEnum.NUMERO_MESI_1_LONG_OPTION.getString(),
                OptionEnum.NUMERO_MESI_1_OPTION_DESCRIPTION.getString());
    }

    public static Option getNumeroMesi2Option(){

        return getRequiredOptionWithArg(OptionEnum.NUMERO_MESI_2_SHORT_OPTION.getString(),
                OptionEnum.NUMERO_MESI_2_LONG_OPTION.getString(),
                OptionEnum.NUMERO_MESI_2_OPTION_DESCRIPTION.getString());
    }
}
