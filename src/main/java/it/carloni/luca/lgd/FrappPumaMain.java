package it.carloni.luca.lgd;

import org.apache.commons.cli.Option;
import it.carloni.luca.lgd.steps.FrappPuma;
import it.carloni.luca.lgd.option.OptionFactory;
import it.carloni.luca.lgd.params.StepParams;

import java.util.Collections;
import java.util.List;

public class FrappPumaMain {

    public static void main(String[] args){

        Option dataAOption = OptionFactory.getDataAOpton();
        List<Option> frappPumaOptionList = Collections.singletonList(dataAOption);
        StepParams stepParams = new StepParams(args, frappPumaOptionList);

        FrappPuma frappPuma = new FrappPuma(stepParams.getDataA());
        frappPuma.run();
    }
}