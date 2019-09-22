package steps;

import org.apache.commons.cli.*;
import steps.lgdstep.QuadFposi;

import java.util.logging.Logger;

public class QuadFposiMain {

    private static Logger logger = Logger.getLogger(QuadFposiMain.class.getName());

    public static void main(String[] args){

        // define options for $ufficio, then set it as required
        Option ufficioOption = new Option("u", "ufficio", true, "parametro $ufficio");
        ufficioOption.setRequired(true);

        Options quadFposiOptions = new Options();
        quadFposiOptions.addOption(ufficioOption);

        CommandLineParser commandLineParser = new BasicParser();

        String ufficio;
        try {

            CommandLine commandLine = commandLineParser.parse(quadFposiOptions, args);
            ufficio = commandLine.getOptionValue("ufficio");
        }
        catch (ParseException e) {

            logger.info("ParseException: " + e.getMessage());
            ufficio = "defaultUfficio";
        }

        QuadFposi quadFposi = new QuadFposi(ufficio);
        quadFposi.run();
    }
}
