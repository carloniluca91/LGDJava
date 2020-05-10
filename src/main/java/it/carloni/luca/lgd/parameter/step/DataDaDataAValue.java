package it.carloni.luca.lgd.parameter.step;

import it.carloni.luca.lgd.option.OptionEnum;
import it.carloni.luca.lgd.parameter.common.AbstractStepValue;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public class DataDaDataAValue extends AbstractStepValue {

    @Getter private String dataDa;
    @Getter private String dataA;

    @Override
    public String toString() {

        String dataDaDescription = OptionEnum.DATA_DA_OPTION_DESCRIPTION.getString();
        String dataADescription = OptionEnum.DATA_A_OPTION_DESCRIPTION.getString();
        return String.format("%s : %s, %s: %s", dataDaDescription, dataDa, dataADescription, dataA);
    }
}
