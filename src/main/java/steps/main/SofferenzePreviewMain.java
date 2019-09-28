package steps.main;

import org.apache.commons.cli.*;
import steps.lgdstep.SofferenzePreview;

import java.util.logging.Logger;

public class SofferenzePreviewMain {

    private static Logger logger = Logger.getLogger(SofferenzePreviewMain.class.getName());

    public static void main(String[] args){

        // define options for $ufficio and  $data_a, then set them as required
        Option ufficioOption = new Option("u", "ufficio", true, "parametro $ufficio");
        Option dataAOption = new Option("dA", "dataA", true, "parametro $data_a");
        ufficioOption.setRequired(true);
        dataAOption.setRequired(true);

        // add the two options
        Options sofferenzePreviewOptions = new Options();
        sofferenzePreviewOptions.addOption(ufficioOption);
        sofferenzePreviewOptions.addOption(dataAOption);

        CommandLineParser parser = new BasicParser();
        String ufficio, dataA;

        try {

            CommandLine commandLine = parser.parse(sofferenzePreviewOptions, args);
            ufficio = commandLine.getOptionValue("ufficio");
            dataA = commandLine.getOptionValue("dataA");
            logger.info("Arguments parsed correctly");

            SofferenzePreview sofferenzePreview = new SofferenzePreview(ufficio, dataA);
            sofferenzePreview.run();
        }
        catch (ParseException e) {

            logger.info("ParseException: " + e.getMessage());
        }
    }
}
