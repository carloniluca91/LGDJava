package steps.mains;

import org.apache.commons.cli.Option;
import steps.lgdstep.FrappPuma;
import steps.params.OptionFactory;
import steps.params.StepParams;

public class FrappPumaMain {

    public static void main(String[] args) {

        // define options
        Option dataAOption = OptionFactory.getDataAOpton();

        String loggerName = FrappPumaMain.class.getSimpleName();
        StepParams stepParams = new StepParams(loggerName, args, dataAOption);
        FrappPuma frappPuma = new FrappPuma(loggerName, stepParams.getDataA());
        frappPuma.run();
    }
}
