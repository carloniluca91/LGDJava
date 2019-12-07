package steps.mains;

import org.apache.commons.cli.Option;
import steps.lgdstep.CicliLavStep1;
import steps.params.OptionFactory;
import steps.params.StepParams;

public class CicliLavStep1Main {

    public static void main(String[] args){

        // define options
        Option dataDaOption = OptionFactory.getDataDaOption();
        Option dataAOption = OptionFactory.getDataAOpton();

        String loggerName = CicliLavStep1Main.class.getSimpleName();
        StepParams stepParams = new StepParams(loggerName, args, dataDaOption, dataAOption);
        CicliLavStep1 cicliLavStep1 = new CicliLavStep1(loggerName, stepParams.getDataDa(), stepParams.getDataA());
        cicliLavStep1.run();

    }
}
