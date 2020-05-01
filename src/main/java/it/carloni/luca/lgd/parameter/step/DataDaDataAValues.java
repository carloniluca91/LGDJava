package it.carloni.luca.lgd.parameter.step;

import it.carloni.luca.lgd.option.OptionEnum;
import it.carloni.luca.lgd.parameter.common.AbstractStepValues;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public class DataDaDataAValues extends AbstractStepValues {

    @Getter private String dataDa;
    @Getter private String dataA;

    @Override
    public String toString() {

        String dataDaDescription = OptionEnum.DATA_DA_OPTION_DESCRIPTION;
        String dataADescription = OptionEnum.DATA_A_OPTION_DESCRIPTION;
        return String.format("%s : %s, %s: %s", dataDaDescription, dataDa, dataADescription, dataA);
    }
}
