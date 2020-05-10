package it.carloni.luca.lgd.parameter.step;

import it.carloni.luca.lgd.option.OptionEnum;
import it.carloni.luca.lgd.parameter.common.AbstractStepValue;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public class DataAValue extends AbstractStepValue {

    @Getter private String dataA;

    @Override
    public String toString() {

        String dataOptionDescription = OptionEnum.DATA_A_OPTION_DESCRIPTION.getString();
        return String.format("%s: %s", dataOptionDescription, dataA);
    }
}
