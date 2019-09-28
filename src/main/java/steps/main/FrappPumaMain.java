package steps.main;

import org.apache.commons.cli.*;
import steps.lgdstep.FrappPuma;

import java.util.logging.Logger;

public class FrappPumaMain {

    private static Logger logger = Logger.getLogger(FrappPumaMain.class.getName());

    public static void main(String[] args) {

        // define option for $data_a
        Option dataAOption = new Option("da", "dataA", true, "parametro $data_a");

        Options options = new Options();
        options.addOption(dataAOption);

        // try to parse and retrieve command line arguments
        CommandLineParser commandLineParser = new BasicParser();
        String dataA;

        try {

            CommandLine commandLine = commandLineParser.parse(options, args);
            dataA = commandLine.getOptionValue("dataA");
            logger.info("Arguments parsed correctly");

            FrappPuma frappPuma = new FrappPuma(dataA);
            frappPuma.run();
        }
        catch (ParseException e) {

            logger.info("ParseException: " + e.getMessage());
        }
    }
}
