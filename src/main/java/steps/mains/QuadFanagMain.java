package steps.mains;

import org.apache.commons.cli.Option;
import steps.lgdstep.QuadFanag;
import steps.params.OptionFactory;
import steps.params.StepParams;

public class QuadFanagMain {

    public static void main(String[] args){

        // define options
        Option ufficioOption = OptionFactory.getUfficioOption();

        String loggerName = QuadFanagMain.class.getSimpleName();
        StepParams stepParams = new StepParams(loggerName, args, ufficioOption);
        QuadFanag quadFanag = new QuadFanag(loggerName, stepParams.getUfficio());
        quadFanag.run();
    }
}
