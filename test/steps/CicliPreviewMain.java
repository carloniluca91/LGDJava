package steps;

import org.apache.commons.cli.*;
import steps.lgdstep.CicliPreview;

import java.util.logging.Logger;

public class CicliPreviewMain {

    private static Logger logger = Logger.getLogger(CicliPreviewMain.class.getName());

    public static void main(String[] args){

        // define options dataA, ufficio and set them as required
        Option dataAOption = new Option("da", "dataA", true, "parametro $data_a");
        Option ufficioOption = new Option("u", "ufficio", true, "parametro $ufficio");
        dataAOption.setRequired(true);
        ufficioOption.setRequired(true);

        // add the two previously defined options
        Options options = new Options();
        options.addOption(dataAOption);
        options.addOption(ufficioOption);

        CommandLineParser commandLineParser = new BasicParser();

        // try to parse and retrieve command line arguments
        String dataA;
        String ufficio;

        try{

            CommandLine cmd = commandLineParser.parse(options, args);
            dataA = cmd.getOptionValue("dataA");
            ufficio = cmd.getOptionValue("ufficio");
            logger.info("Arguments parsed correctly");

        } catch (ParseException e) {

            // asign some dafault values
            logger.info("ParseException: " + e.getMessage());
            dataA = "2019-01-01";
            ufficio = "ufficio_bpm";
        }

        CicliPreview cicliPreview = new CicliPreview(dataA, ufficio);
        cicliPreview.run();
    }
}
