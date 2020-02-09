import org.apache.commons.cli.Option;
import steps.lgdstep.SofferenzePreview;
import steps.params.OptionFactory;
import steps.params.StepParams;

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
