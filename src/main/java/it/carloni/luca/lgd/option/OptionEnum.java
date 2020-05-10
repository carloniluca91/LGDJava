package it.carloni.luca.lgd.option;

import lombok.Getter;

public enum OptionEnum {

    STEP_NAME_SHORT_OPTION("s"),
    STEP_NAME_LONG_OPTION("step-name"),
    STEP_NAME_OPTION_DESCRIPTION("job Spark da eseguire"),

    DATA_A_SHORT_OPTION("da"),
    DATA_A_LONG_OPTION("data-a"),
    DATA_A_OPTION_DESCRIPTION("parametro $data_a"),

    DATA_DA_SHORT_OPTION("dd"),
    DATA_DA_LONG_OPTION("data-da"),
    DATA_DA_OPTION_DESCRIPTION("parametro $data_da"),

    DATA_OSSERVAZIONE_SHORT_OPTION("dao"),
    DATA_OSSERVAZIONE_LONG_OPTION("data-osservazione"),
    DATA_OSSERVAZIONE_OPTION_DESCRIPTION("parametro $data_osservazione"),

    UFFICIO_SHORT_OPTION("uf"),
    UFFICIO_LONG_OPTION("ufficio"),
    UFFICIO_OPTION_DESCRIPTION("parametro $ufficio"),

    NUMERO_MESI_1_SHORT_OPTION("nm1"),
    NUMERO_MESI_1_LONG_OPTION("numero-mesi-1"),
    NUMERO_MESI_1_OPTION_DESCRIPTION("parametro $numero_mesi_1"),

    NUMERO_MESI_2_SHORT_OPTION("nm2"),
    NUMERO_MESI_2_LONG_OPTION("numero-mesi-2"),
    NUMERO_MESI_2_OPTION_DESCRIPTION("parametro $numero_mesi_2"),

    HELP_USAGE_STRING("spark2-submit ... --class it.carloni.luca.lgd.Main <jar-path>"),
    HELP_HEADER_STRING("LGD main application"),
    HELP_FOOTER_STRING("\nCreated by Carloni Luca");

    @Getter private String string;

    OptionEnum(String value) { this.string = value; }

}
