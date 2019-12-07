package steps.mains;

import org.apache.commons.cli.Option;
import steps.lgdstep.SofferenzePreview;
import steps.params.OptionFactory;
import steps.params.StepParams;

public class SofferenzePreviewMain {

    public static void main(String[] args){

        // define options
        Option ufficioOption = OptionFactory.getUfficioOption();
        Option dataAOption = OptionFactory.getDataAOpton();

        String loggerName = SofferenzePreviewMain.class.getSimpleName();
        StepParams stepParams = new StepParams(loggerName, args, ufficioOption, dataAOption);
        SofferenzePreview sofferenzePreview = new SofferenzePreview(loggerName, stepParams.getUfficio(), stepParams.getDataA());
        sofferenzePreview.run();

    }
}
