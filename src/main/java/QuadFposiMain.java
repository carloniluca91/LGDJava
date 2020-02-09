import org.apache.commons.cli.Option;
import steps.lgdstep.QuadFposi;
import steps.params.OptionFactory;
import steps.params.StepParams;

import java.util.Collections;
import java.util.List;

public class QuadFposiMain {

    public static void main(String[] args){

        Option ufficioOption = OptionFactory.getUfficioOption();
        List<Option> quadFposiCicliOptionList = Collections.singletonList(ufficioOption);
        StepParams stepParams = new StepParams(args, quadFposiCicliOptionList);

        QuadFposi quadFposi = new QuadFposi(stepParams.getUfficio());
        quadFposi.run();
    }
}
