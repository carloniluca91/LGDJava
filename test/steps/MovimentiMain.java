package steps;

import org.apache.commons.cli.*;
import steps.lgdstep.Movimenti;

import java.util.logging.Logger;

public class MovimentiMain {

    private static Logger logger = Logger.getLogger(MovimentiMain.class.getName());

    public static void main(String[] args){

        Option dataOsservazioneOption = new Option("dO", "dataOsservazione", true, "parametro $data_osservazione");
        dataOsservazioneOption.setRequired(true);

        Options movimentiOptions = new Options();
        movimentiOptions.addOption(dataOsservazioneOption);

        CommandLineParser commandLineParser = new BasicParser();

        String dataOsservazione;
        try {

            CommandLine commandLine = commandLineParser.parse(movimentiOptions, args);
            dataOsservazione = commandLine.getOptionValue("dataOsservazione");

        } catch (ParseException e) {

            logger.info("ParseException: " + e.getMessage());
            dataOsservazione = "2018-01-01";
        }

        Movimenti movimenti = new Movimenti(dataOsservazione);
        movimenti.run();
    }
}
