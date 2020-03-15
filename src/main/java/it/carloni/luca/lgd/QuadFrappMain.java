package it.carloni.luca.lgd;

import org.apache.commons.cli.Option;
import it.carloni.luca.lgd.steps.QuadFrapp;
import it.carloni.luca.lgd.options.OptionFactory;
import it.carloni.luca.lgd.params.StepParams;

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
