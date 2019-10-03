package steps.main;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import steps.lgdstep.QuadFrapp;

import java.util.logging.Logger;

public class QuadFrappMain {

    private static Logger logger = Logger.getLogger(QuadFrappMain.class.getName());

    public static void main(String[] args){

        Option ufficioOption = new Option("u", "ufficio", true, "parametro $ufficio");
        ufficioOption.setRequired(true);

        Options quadFrappOtions = new Options();
        quadFrappOtions.addOption(ufficioOption);

        CommandLineParser commandLineParser = new BasicParser();

        String ufficio;

        try {

            CommandLine commandLine = commandLineParser.parse(quadFrappOtions, args);
            ufficio = commandLine.getOptionValue("ufficio");
            logger.info("$ufficio: " + ufficio);
            QuadFrapp quadFrapp = new QuadFrapp(ufficio);
            quadFrapp.run();

        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
