package steps.main;

import org.apache.commons.cli.*;
import steps.lgdstep.FanagMonthly;

import java.util.logging.Logger;

public class FanagMonthlyMain {

    private static Logger logger = Logger.getLogger(FanagMonthlyMain.class.getName());

    public static void main(String[] args){

        Options options = new Options();

        Option numeroMesi1Option = new Option("nm1", "numero-mesi-1", true, "parametro $numero_mesi_1");
        Option numeroMesi2Option = new Option("nm2", "numero-mesi-2", true, "parametro $numero_mesi_2");
        Option dataAOption = new Option("dA", "dataA", true, "parametro $data_a");

        numeroMesi1Option.setRequired(true);
        numeroMesi2Option.setRequired(true);
        dataAOption.setRequired(true);

        options.addOption(numeroMesi1Option);
        options.addOption(numeroMesi2Option);
        options.addOption(dataAOption);

        int numeroMesi1, numeroMesi2;
        String dataA;

        try {

            CommandLineParser commandLineParser = new BasicParser();
            CommandLine commandLine = commandLineParser.parse(options, args);
            numeroMesi1 = Integer.parseInt(commandLine.getOptionValue("numero-mesi-1"));
            numeroMesi2 = Integer.parseInt(commandLine.getOptionValue("numero-mesi-2"));
            dataA = commandLine.getOptionValue("dataA");

            logger.info("numeroMesi1: " + numeroMesi1);
            logger.info("numeroMesi2: " + numeroMesi2);
            logger.info("dataA: " + dataA);

            FanagMonthly fanagMonthly = new FanagMonthly(numeroMesi1, numeroMesi2, dataA);
            fanagMonthly.run();

        } catch (ParseException e) {

            logger.info(e.getMessage());
        }
    }
}
