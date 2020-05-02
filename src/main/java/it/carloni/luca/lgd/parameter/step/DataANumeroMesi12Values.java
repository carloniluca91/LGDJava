package it.carloni.luca.lgd.parameter.step;

import it.carloni.luca.lgd.option.OptionEnum;
import it.carloni.luca.lgd.parameter.common.AbstractStepValues;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public class DataANumeroMesi12Values extends AbstractStepValues {

    @Getter private String dataA;
    @Getter private Integer numeroMesi1;
    @Getter private Integer numeroMesi2;

    @Override
    public String toString() {

        String dataADescription = OptionEnum.DATA_A_OPTION_DESCRIPTION;
        String numeroMesi1Description = OptionEnum.NUMERO_MESI_1_OPTION_DESCRIPTION;
        String numeroMesi2Description = OptionEnum.NUMERO_MESI_2_OPTION_DESCRIPTION;

        return String.format("%s: %s, %s: %s, %s: %s", dataADescription, dataA,
                numeroMesi1Description, numeroMesi1, numeroMesi2Description, numeroMesi2);

    }
}
