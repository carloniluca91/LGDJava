import org.apache.commons.cli.Option;
import steps.lgdstep.FrappNdgMonthly;
import steps.params.OptionFactory;
import steps.params.StepParams;

import java.util.Arrays;
import java.util.List;

public class FrappNdgMonthlyMain {

    public static void main(String[] args){

        Option dataAOpton = OptionFactory.getDataAOpton();
        Option numeroMesi1Option = OptionFactory.getNumeroMesi1Option();
        Option numeroMesi2Option = OptionFactory.getNumeroMesi2Option();
        List<Option> frappNdgMonthlyOptionList = Arrays.asList(dataAOpton, numeroMesi1Option, numeroMesi2Option);
        StepParams stepParams = new StepParams(args, frappNdgMonthlyOptionList);

        FrappNdgMonthly frappNdgMonthly = new FrappNdgMonthly(stepParams.getDataA(),
                stepParams.getNumeroMesi1(), stepParams.getNumeroMesi2());
        frappNdgMonthly.run();
    }
}
