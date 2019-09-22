package steps;

import org.apache.commons.cli.*;
import steps.lgdstep.FrappNdgMonthly;

import java.util.logging.Logger;

public class FrappNdgMonthlyMain {

    private static Logger logger = Logger.getLogger(FrappNdgMonthlyMain.class.getName());

    public static void main(String[] args) {

        // define option dataA, periodo, numeroMesi1, numeroMesi2
        Option dataAOption = new Option("da", "dataA", true, "parametro $data_a");
        Option numeroMesi1Option = new Option("nm_uno", "numero_mesi_1", true, "parametro $numero_mesi_1");
        Option numeroMesi2Option = new Option("nm_due", "numero_mesi_2", true, "parametro $numero_mesi_2");

        // set them as required
        dataAOption.setRequired(true);
        numeroMesi1Option.setRequired(true);
        numeroMesi2Option.setRequired(true);

        // add them to Options
        Options options = new Options();
        options.addOption(dataAOption);
        options.addOption(numeroMesi1Option);
        options.addOption(numeroMesi2Option);

        CommandLineParser commandLineParser = new BasicParser();

        int numeroMesi1, numeroMesi2;
        String dataA;

        // try to parse and retrieve command line arguments
        try{

            CommandLine commandLine = commandLineParser.parse(options, args);
            dataA = commandLine.getOptionValue("dataA");
            numeroMesi1 = Integer.parseInt(commandLine.getOptionValue("numero_mesi_1"));
            numeroMesi2 = Integer.parseInt(commandLine.getOptionValue("numero_mesi_2"));
            logger.info("Arguments parsed correctly");

        }
        catch (ParseException e) {

            logger.info("ParseException: " + e.getMessage());
            dataA = "2018-12-01";
            numeroMesi1 = 1;
            numeroMesi2 = 2;
        }

        FrappNdgMonthly frapp = new FrappNdgMonthly(dataA, numeroMesi1, numeroMesi2);
        frapp.run();
    }
}
