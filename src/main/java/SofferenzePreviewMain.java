import org.apache.commons.cli.Option;
import it.carloni.luca.lgd.steps.SofferenzePreview;
import it.carloni.luca.lgd.params.OptionFactory;
import it.carloni.luca.lgd.params.StepParams;

import java.util.Arrays;
import java.util.List;

public class SofferenzePreviewMain {

    public static void main(String[] args){

        Option ufficioOption = OptionFactory.getUfficioOption();
        Option dataAOpton = OptionFactory.getDataAOpton();
        List<Option> sofferenzePreviewOptionList = Arrays.asList(ufficioOption, dataAOpton);
        StepParams stepParams = new StepParams(args, sofferenzePreviewOptionList);

        SofferenzePreview sofferenzePreview = new SofferenzePreview(stepParams.getUfficio(), stepParams.getDataA());
        sofferenzePreview.run();
    }
}
