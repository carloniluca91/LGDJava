package it.carloni.luca.lgd.parameter.step;

import it.carloni.luca.lgd.option.OptionEnum;
import it.carloni.luca.lgd.parameter.common.AbstractStepValue;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public class DataANumeroMesi12Value extends AbstractStepValue {

    @Getter private String dataA;
    @Getter private Integer numeroMesi1;
    @Getter private Integer numeroMesi2;

    @Override
    public String toString() {

        String dataADescription = OptionEnum.DATA_A_OPTION_DESCRIPTION.getString();
        String numeroMesi1Description = OptionEnum.NUMERO_MESI_1_OPTION_DESCRIPTION.getString();
        String numeroMesi2Description = OptionEnum.NUMERO_MESI_2_OPTION_DESCRIPTION.getString();

        return String.format("%s: %s, %s: %s, %s: %s", dataADescription, dataA,
                numeroMesi1Description, numeroMesi1, numeroMesi2Description, numeroMesi2);

    }
}
