package steps.mains;

import org.apache.commons.cli.Option;
import steps.lgdstep.CicliLavStep1;
import steps.params.OptionFactory;
import steps.params.StepParams;

import java.util.Arrays;
import java.util.List;

public class CicliLavStep1Main {

    public static void main(String[] args){

        // DEFINE STEP OPTIONS
        Option dataDaOption = OptionFactory.getDataDaOption();
        Option dataAOption = OptionFactory.getDataAOpton();
        List<Option> cicliLavstep1OptionList = Arrays.asList(dataDaOption, dataAOption);

        StepParams stepParams = new StepParams(args, cicliLavstep1OptionList);
        CicliLavStep1 cicliLavStep1 = new CicliLavStep1(stepParams.getDataDa(), stepParams.getDataA());
        cicliLavStep1.run();
    }
}
