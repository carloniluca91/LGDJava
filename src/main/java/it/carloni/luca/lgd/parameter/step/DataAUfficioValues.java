package it.carloni.luca.lgd.parameter.step;

import it.carloni.luca.lgd.option.OptionEnum;
import it.carloni.luca.lgd.parameter.common.AbstractStepValues;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public class DataAUfficioValues extends AbstractStepValues {

    @Getter private String dataA;
    @Getter private String ufficio;

    @Override
    public String toString() {

        String dataADescription = OptionEnum.DATA_A_OPTION_DESCRIPTION;
        String ufficioDescription = OptionEnum.UFFICIO_OPTION_DESCRIPTION;
        return String.format("%s: %s, %s: %s", dataADescription, dataA, ufficioDescription, ufficio);
    }
}
