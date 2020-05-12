package it.carloni.luca.lgd.parameter.step;

import it.carloni.luca.lgd.option.OptionEnum;
import it.carloni.luca.lgd.parameter.common.AbstractStepValue;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public class DataOsservazioneValue extends AbstractStepValue {

    @Getter private String dataosservazione;

    @Override
    public String toString() {

        String dataOsservazioneDescription = OptionEnum.DATA_OSSERVAZIONE_OPTION_DESCRIPTION.getString();
        return String.format("%s: %s", dataOsservazioneDescription, dataosservazione);
    }
}
