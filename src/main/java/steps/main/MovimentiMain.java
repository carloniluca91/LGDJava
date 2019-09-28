package steps.main;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
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
            logger.info("dataOsservazione: " + dataOsservazione);
            Movimenti movimenti = new Movimenti(dataOsservazione);
            movimenti.run();

        } catch (ParseException e) {

            logger.info("ParseException: " + e.getMessage());
        }
    }
}
