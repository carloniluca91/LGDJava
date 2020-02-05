import org.apache.commons.cli.Option;
import steps.lgdstep.CicliPreview;
import steps.params.OptionFactory;
import steps.params.StepParams;

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
