package steps.main;

import org.apache.commons.cli.*;
import steps.lgdstep.QuadFanag;

import java.util.logging.Logger;

public class QuadFanagMain {

    private static Logger logger = Logger.getLogger(QuadFposiMain.class.getName());

    public static void main(String[] args){

        // define options for $ufficio, then set it as required
        Option ufficioOption = new Option("u", "ufficio", true, "parametro $ufficio");
        ufficioOption.setRequired(true);

        Options quadFanagOptions = new Options();
        quadFanagOptions.addOption(ufficioOption);

        CommandLineParser commandLineParser = new BasicParser();

        String ufficio;
        try {

            CommandLine commandLine = commandLineParser.parse(quadFanagOptions, args);
            ufficio = commandLine.getOptionValue("ufficio");
            QuadFanag quadFanag = new QuadFanag(ufficio);
            quadFanag.run();
        }
        catch (ParseException e) {

            logger.info("ParseException: " + e.getMessage());
        }
    }
}
