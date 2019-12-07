package steps.mains;

import org.apache.commons.cli.Option;
import steps.lgdstep.QuadFcollCicli;
import steps.params.OptionFactory;
import steps.params.StepParams;

public class QuadFcollCicliMain {

    public static void main(String[] args){

        // define options
        Option ufficioOption = OptionFactory.getUfficioOption();

        String loggerName = QuadFcollCicliMain.class.getSimpleName();
        StepParams stepParams = new StepParams(loggerName, args, ufficioOption);
        QuadFcollCicli quadFcollCicli = new QuadFcollCicli(loggerName, stepParams.getUfficio());
        quadFcollCicli.run();
    }
}
