package it.carloni.luca.lgd;

import org.apache.commons.cli.Option;
import it.carloni.luca.lgd.steps.Movimenti;
import it.carloni.luca.lgd.options.OptionFactory;
import it.carloni.luca.lgd.params.StepParams;

import java.util.Collections;
import java.util.List;

public class MovimentiMain {

    public static void main(String[] args){

        Option dataOsservazioneOption = OptionFactory.getDataOsservazioneOption();
        List<Option> movimentiOptionList = Collections.singletonList(dataOsservazioneOption);
        StepParams stepParams = new StepParams(args, movimentiOptionList);

        Movimenti movimenti = new Movimenti(stepParams.getDataOsservazione());
        movimenti.run();
    }
}
