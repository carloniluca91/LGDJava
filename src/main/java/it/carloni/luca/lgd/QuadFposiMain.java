package it.carloni.luca.lgd;

import org.apache.commons.cli.Option;
import it.carloni.luca.lgd.spark.steps.QuadFposi;
import it.carloni.luca.lgd.options.OptionFactory;
import it.carloni.luca.lgd.params.StepParams;

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
