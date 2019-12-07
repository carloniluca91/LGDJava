package steps.main;

import org.apache.commons.cli.Option;
import steps.lgdstep.CicliPreview;
import steps.params.OptionFactory;
import steps.params.StepParams;

public class CicliPreviewMain {

    public static void main(String[] args){

        // define options
        Option dataAOption = OptionFactory.getDataAOpton();
        Option ufficioOption = OptionFactory.getUfficioOption();

        String loggerName = CicliPreviewMain.class.getSimpleName();
        StepParams stepParams = new StepParams(loggerName, args, dataAOption, ufficioOption);
        CicliPreview cicliPreview = new CicliPreview(loggerName, stepParams.getDataA(), stepParams.getUfficio());
        cicliPreview.run();
    }
}
