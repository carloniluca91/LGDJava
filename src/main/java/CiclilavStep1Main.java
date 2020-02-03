import org.apache.commons.cli.Option;
import steps.lgdstep.CiclilavStep1;
import steps.params.OptionFactory;
import steps.params.StepParams;

import java.util.Arrays;
import java.util.List;

public class CiclilavStep1Main {

    public static void main(String[] args){

        // DEFINE STEP OPTIONS
        Option dataDaOption = OptionFactory.getDataDaOption();
        Option dataAOption = OptionFactory.getDataAOpton();
        List<Option> cicliLavstep1OptionList = Arrays.asList(dataDaOption, dataAOption);

        StepParams stepParams = new StepParams(args, cicliLavstep1OptionList);
        CiclilavStep1 ciclilavStep1 = new CiclilavStep1(stepParams.getDataDa(), stepParams.getDataA());
        ciclilavStep1.run();
    }
}
