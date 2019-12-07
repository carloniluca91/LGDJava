package steps.main;

import org.apache.commons.cli.Option;
import steps.lgdstep.FanagMonthly;
import steps.params.OptionFactory;
import steps.params.StepParams;

public class FanagMonthlyMain {

    public static void main(String[] args){

        // define options
        Option numeroMesi1Option = OptionFactory.getNumeroMesi1Option();
        Option numeroMesi2Option = OptionFactory.getNumeroMesi2Option();
        Option dataAOption = OptionFactory.getDataAOpton();

        String loggerName = FanagMonthlyMain.class.getSimpleName();
        StepParams stepParams = new StepParams(loggerName, args, numeroMesi1Option, numeroMesi2Option, dataAOption);
        FanagMonthly fanagMonthly = new FanagMonthly(loggerName, stepParams.getNumeroMesi1(), stepParams.getNumeroMesi2(), stepParams.getDataA());
        fanagMonthly.run();
    }
}
