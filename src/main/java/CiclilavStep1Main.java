import org.apache.commons.cli.Option;
import steps.lgdstep.CiclilavStep1;
import steps.params.OptionFactory;
import steps.params.StepParams;

import java.util.Arrays;
import java.util.List;

public class CiclilavStep1Main{

    public static void main(String[] args){

        // OPTIONS REQUIRED
        Option dataDaOption = OptionFactory.getDataDaOption();
        Option dataAOpton = OptionFactory.getDataAOpton();
        List<Option> ciclilavStep1Options = Arrays.asList(dataDaOption, dataAOpton);

        StepParams stepParams = new StepParams(args, ciclilavStep1Options);
        CiclilavStep1 ciclilavStep1 = new CiclilavStep1(stepParams.getDataDa(),stepParams.getDataA());
        ciclilavStep1.run();
    }
}