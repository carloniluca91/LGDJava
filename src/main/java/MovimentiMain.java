import org.apache.commons.cli.Option;
import steps.lgdstep.Movimenti;
import steps.params.OptionFactory;
import steps.params.StepParams;

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
