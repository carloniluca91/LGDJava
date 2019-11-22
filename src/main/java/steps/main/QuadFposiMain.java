package steps.main;

import org.apache.commons.cli.Option;
import steps.lgdstep.QuadFposi;
import steps.params.OptionFactory;
import steps.params.StepParams;

public class QuadFposiMain {

    public static void main(String[] args){

        // define options
        Option ufficioOption = OptionFactory.getUfficioOption();

        String loggerName = QuadFposiMain.class.getSimpleName();
        StepParams stepParams = new StepParams(loggerName, args, ufficioOption);
        QuadFposi quadFposi = new QuadFposi(loggerName, stepParams.getUfficio());
        quadFposi.run();
    }
}
