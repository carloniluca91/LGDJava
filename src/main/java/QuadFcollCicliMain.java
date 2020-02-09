import org.apache.commons.cli.Option;
import steps.lgdstep.QuadFcollCicli;
import steps.params.OptionFactory;
import steps.params.StepParams;

import java.util.Collections;
import java.util.List;

public class QuadFcollCicliMain {

    public static void main(String[] args){

        Option ufficioOption = OptionFactory.getUfficioOption();
        List<Option> quadFcollCicliOptionList = Collections.singletonList(ufficioOption);
        StepParams stepParams = new StepParams(args, quadFcollCicliOptionList);

        QuadFcollCicli quadFcollCicli = new QuadFcollCicli(stepParams.getUfficio());
        quadFcollCicli.run();

    }
}
