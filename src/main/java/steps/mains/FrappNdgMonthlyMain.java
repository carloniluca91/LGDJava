package steps.mains;

import org.apache.commons.cli.Option;
import steps.lgdstep.FrappNdgMonthly;
import steps.params.OptionFactory;
import steps.params.StepParams;

public class FrappNdgMonthlyMain {

    public static void main(String[] args) {

        // define options
        Option dataAOption = OptionFactory.getDataAOpton();
        Option numeroMesi1Option = OptionFactory.getNumeroMesi1Option();
        Option numeroMesi2Option = OptionFactory.getNumeroMesi2Option();

        String loggerName = FrappNdgMonthlyMain.class.getSimpleName();
        StepParams stepParams = new StepParams(loggerName, args, dataAOption, numeroMesi1Option, numeroMesi2Option);
        FrappNdgMonthly frappNdgMonthly = new FrappNdgMonthly(loggerName, stepParams.getDataA(), stepParams.getNumeroMesi1(),
                stepParams.getNumeroMesi2());

        frappNdgMonthly.run();
    }
}
