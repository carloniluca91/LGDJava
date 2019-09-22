package steps;

import org.apache.commons.cli.*;
import steps.lgdstep.CicliLavStep1;

import java.util.logging.Logger;

public class CicliLavStep1Main {

    private static Logger logger = Logger.getLogger(CicliLavStep1Main.class.getName());

    public static void main(String[] args){

        // define options dataDa and dataA and set them as required
        Option dataDaOption = new Option("dd", "dataDa", true, "parametro $data_da");
        Option dataAOption = new Option("da", "dataA", true, "parametro $data_a");
        dataDaOption.setRequired(true);
        dataAOption.setRequired(true);

        // add the two previously defined options
        Options options = new Options();
        options.addOption(dataDaOption);
        options.addOption(dataAOption);

        CommandLineParser commandLineParser = new BasicParser();

        // try to parse and retrieve command line arguments
        String dataDa;
        String dataA;
        try {

            CommandLine commandLine = commandLineParser.parse(options, args);
            dataDa = commandLine.getOptionValue("dataDa");
            dataA = commandLine.getOptionValue("dataA");
            logger.info("Arguments parsed correctly");

        } catch (ParseException e) {

            // assign some default values
            logger.info("ParseException: " + e.getMessage());
            dataDa = "2015-01-01";
            dataA = "2019-01-01";
        }

        CicliLavStep1 cicliLavStep1 = new CicliLavStep1(dataDa, dataA);
        cicliLavStep1.run();
    }
}
