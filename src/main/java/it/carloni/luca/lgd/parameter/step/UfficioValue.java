package it.carloni.luca.lgd.parameter.step;

import it.carloni.luca.lgd.option.OptionEnum;
import it.carloni.luca.lgd.parameter.common.AbstractStepValue;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public class UfficioValue extends AbstractStepValue {

    @Getter private String ufficio;

    @Override
    public String toString() {

        String description = OptionEnum.UFFICIO_OPTION_DESCRIPTION.getString();
        return String.format("%s: %s", description, ufficio);
    }
}
