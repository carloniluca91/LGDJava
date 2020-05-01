package it.carloni.luca.lgd;

import org.apache.commons.cli.Option;
import it.carloni.luca.lgd.spark.steps.QuadFcollCicli;
import it.carloni.luca.lgd.options.OptionFactory;
import it.carloni.luca.lgd.params.StepParams;

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
