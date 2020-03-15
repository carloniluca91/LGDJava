package it.carloni.luca.lgd;

import org.apache.commons.cli.Option;
import it.carloni.luca.lgd.steps.CicliPreview;
import it.carloni.luca.lgd.options.OptionFactory;
import it.carloni.luca.lgd.params.StepParams;

import java.util.Arrays;
import java.util.List;

public class CicliPreviewMain {

    public static void main(String[] args){

        // OPTIONS REQUIRED
        Option dataAOption = OptionFactory.getDataAOpton();
        Option ufficioOption = OptionFactory.getUfficioOption();
        List<Option> cicliPreviewOptionList = Arrays.asList(dataAOption, ufficioOption);

        StepParams stepParams = new StepParams(args, cicliPreviewOptionList);
        CicliPreview cicliPreview = new CicliPreview(stepParams.getDataA(), stepParams.getUfficio());
        cicliPreview.run();
    }
}
