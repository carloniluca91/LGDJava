import org.apache.commons.cli.Option;
import steps.lgdstep.FanagMonthly;
import steps.params.OptionFactory;
import steps.params.StepParams;

import java.util.Arrays;
import java.util.List;

public class FanagMonthlyMain {

    public static void main(String[] args){

        Option numeroMesi1Option = OptionFactory.getNumeroMesi1Option();
        Option numeroMesi2Option = OptionFactory.getNumeroMesi2Option();
        Option dataAOpton = OptionFactory.getDataAOpton();
        List<Option> fanagMonthlyOptionList = Arrays.asList(numeroMesi1Option, numeroMesi2Option, dataAOpton);
        StepParams stepParams = new StepParams(args, fanagMonthlyOptionList);

        FanagMonthly fanagMonthly = new FanagMonthly(
                stepParams.getNumeroMesi1(), stepParams.getNumeroMesi2(), stepParams.getDataA());
        fanagMonthly.run();
    }
}
