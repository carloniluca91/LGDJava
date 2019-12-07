package steps.mains;

import org.apache.commons.cli.Option;
import steps.lgdstep.Movimenti;
import steps.params.OptionFactory;
import steps.params.StepParams;

public class MovimentiMain {

    public static void main(String[] args){

        // define options
        Option dataOsservazioneOption = OptionFactory.getDataOsservazioneOption();

        String loggerName = MovimentiMain.class.getSimpleName();
        StepParams stepParams = new StepParams(loggerName, args, dataOsservazioneOption);
        Movimenti movimenti = new Movimenti(loggerName, stepParams.getDataOsservazione());
        movimenti.run();
    }
}
