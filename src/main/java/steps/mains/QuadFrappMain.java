package steps.mains;

import org.apache.commons.cli.Option;
import steps.lgdstep.QuadFrapp;
import steps.params.OptionFactory;
import steps.params.StepParams;

public class QuadFrappMain {

    public static void main(String[] args){

        // define options
        Option ufficioOption = OptionFactory.getUfficioOption();

        String loggerName = QuadFrappMain.class.getSimpleName();
        StepParams stepParams = new StepParams(loggerName, args, ufficioOption);
        QuadFrapp quadFrapp = new QuadFrapp(loggerName, stepParams.getUfficio());
        quadFrapp.run();

    }
}
