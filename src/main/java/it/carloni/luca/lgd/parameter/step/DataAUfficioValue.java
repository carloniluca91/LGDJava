package it.carloni.luca.lgd.parameter.step;

import it.carloni.luca.lgd.option.OptionEnum;
import it.carloni.luca.lgd.parameter.common.AbstractStepValue;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public class DataAUfficioValue extends AbstractStepValue {

    @Getter private String dataA;
    @Getter private String ufficio;

    @Override
    public String toString() {

        String dataADescription = OptionEnum.DATA_A_OPTION_DESCRIPTION.getString();
        String ufficioDescription = OptionEnum.UFFICIO_OPTION_DESCRIPTION.getString();
        return String.format("%s: %s, %s: %s", dataADescription, dataA, ufficioDescription, ufficio);
    }
}
