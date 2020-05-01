package it.carloni.luca.lgd.params;

import it.carloni.luca.lgd.options.OptionFactory;
import lombok.Getter;
import org.apache.commons.cli.*;
import org.apache.log4j.Logger;

import java.util.List;

public class StepParams {

    private final Logger logger  = Logger.getLogger(getClass());
    private final Options stepOptions = new Options();

    @Getter private String dataDa;
    @Getter private String dataA;
    @Getter private String ufficio;
    @Getter private Integer numeroMesi1;
    @Getter private Integer numeroMesi2;
    @Getter private String dataOsservazione;

    public StepParams(String[] args, List<Option> stepOptionList){

        for (Option option: stepOptionList){

            stepOptions.addOption(option);
        }

        parseArgs(args);
    }

    private void parseArgs(String[] args){

        try {

            CommandLineParser commandLineParser = new BasicParser();
            CommandLine commandLine = commandLineParser.parse(stepOptions, args);

            dataDa = parseOptionOfTypeString(commandLine, OptionFactory.getDataDaOption());
            dataA = parseOptionOfTypeString(commandLine, OptionFactory.getDataAOpton());
            ufficio = parseOptionOfTypeString(commandLine, OptionFactory.getUfficioOption());
            numeroMesi1 = parseOptionOfTypeInt(commandLine, OptionFactory.getNumeroMesi1Option());
            numeroMesi2 = parseOptionOfTypeInt(commandLine, OptionFactory.getNumeroMesi2Option());
            dataOsservazione = parseOptionOfTypeString(commandLine, OptionFactory.getDataOsservazioneOption());

            logger.info("Arguments parsed correctly");

        } catch (ParseException e) {

            logger.error("ParseException occurred");
            logger.error(e);
        }
    }

    private Integer parseOptionOfTypeInt(CommandLine commandLine, Option optionToParse) {

        if (stepOptions.hasOption(optionToParse.getLongOpt())) {

            int optionValue = Integer.parseInt(commandLine.getOptionValue(optionToParse.getLongOpt()));
            logger.info(String.format("%s: %s", optionToParse.getDescription(), optionValue));
            return optionValue;
        }

        else return null;
    }

    private String parseOptionOfTypeString(CommandLine commandLine, Option optionToParse) {

        if (stepOptions.hasOption(optionToParse.getLongOpt())){

            String optionValue = commandLine.getOptionValue(optionToParse.getLongOpt());
            logger.info(String.format("%s: %s", optionToParse.getDescription(), optionValue));
            return optionValue;
        }

        else return null;
    }
}
