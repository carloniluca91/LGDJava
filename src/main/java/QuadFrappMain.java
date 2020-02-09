import org.apache.commons.cli.Option;
import steps.lgdstep.QuadFrapp;
import steps.params.OptionFactory;
import steps.params.StepParams;

import java.util.Collections;
import java.util.List;

public class QuadFrappMain {

    public static void main(String[] args){

        Option ufficioOption = OptionFactory.getUfficioOption();
        List<Option> quandFrappOptionList = Collections.singletonList(ufficioOption);
        StepParams stepParams = new StepParams(args, quandFrappOptionList);

        QuadFrapp quadFrapp = new QuadFrapp(stepParams.getUfficio());
        quadFrapp.run();
    }
}
