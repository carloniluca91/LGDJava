package steps.main;

import org.apache.commons.cli.*;
import steps.lgdstep.QuadFcollCicli;

import java.util.logging.Logger;

public class QuadFcollCicliMain {

    private static Logger logger = Logger.getLogger(QuadFcollCicliMain.class.getName());

    public static void main(String[] args){

        Option ufficioOption = new Option("u", "ufficio", true, "parametro $ufficio");
        ufficioOption.setRequired(true);

        Options options = new Options();
        options.addOption(ufficioOption);

        String ufficio;
        CommandLineParser commandLineParser = new BasicParser();
        try {

            CommandLine commandLine = commandLineParser.parse(options, args);
            ufficio = commandLine.getOptionValue("ufficio");
            logger.info("$ufficio: " + ufficio);

            QuadFcollCicli quadFcollCicli = new QuadFcollCicli(ufficio);
            quadFcollCicli.run();

        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
