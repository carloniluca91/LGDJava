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
    @Getter private int numeroMesi1;
    @Getter private int numeroMesi2;
    @Getter private String dataOsservazione;

    private CommandLine commandLine;

    public StepParams(String[] args, List<Option> stepOptionList){

        for (Option option: stepOptionList){

            option.setRequired(true);
            stepOptions.addOption(option);
        }

        parseArgs(args);
    }

    private void parseArgs(String[] args){

        try {

            CommandLineParser commandLineParser = new BasicParser();
            commandLine = commandLineParser.parse(stepOptions, args);

            parseDataDa();
            parseDataA();
            parseUfficio();
            parseNumeroMesi1();
            parseNumeroMesi2();
            parseDataOsservazione();

            logger.info("Arguments parsed correctly");

        } catch (ParseException e) {

            logger.error("ParseException occurred");
            logger.error("e.getMessage(): " + e.getMessage());
            logger.error(e);
        }
    }

    private void parseDataA(){

        Option option = OptionFactory.getDataAOpton();
        if (stepOptions.hasOption(option.getLongOpt())){

            dataA = commandLine.getOptionValue(option.getLongOpt());
            logger.debug(String.format("%s: %s", option.getDescription(), dataA));
        }
    }

    private void parseDataDa(){

        Option option = OptionFactory.getDataDaOption();
        if (stepOptions.hasOption(option.getLongOpt())){

            dataDa = commandLine.getOptionValue(option.getLongOpt());
            logger.debug(String.format("%s: %s", option.getDescription(), dataDa));
        }
    }

    private void parseDataOsservazione(){

        Option option = OptionFactory.getDataOsservazioneOption();
        if (stepOptions.hasOption(option.getLongOpt())){

            dataOsservazione = commandLine.getOptionValue(option.getLongOpt());
            logger.debug(String.format("%s: %s", option.getDescription(), dataOsservazione));
        }
    }

    private void parseNumeroMesi1(){

        Option option = OptionFactory.getNumeroMesi1Option();
        if (stepOptions.hasOption(option.getLongOpt())){

            numeroMesi1 = Integer.parseInt(commandLine.getOptionValue(option.getLongOpt()));
            logger.debug(String.format("%s: %s", option.getDescription(), numeroMesi1));
        }
    }

    private void parseNumeroMesi2(){

        Option option = OptionFactory.getNumeroMesi2Option();
        if (stepOptions.hasOption(option.getLongOpt())){

            numeroMesi2 = Integer.parseInt(commandLine.getOptionValue(option.getLongOpt()));
            logger.debug(String.format("%s: %s", option.getDescription(), numeroMesi2));
        }
    }

    private void parseUfficio(){

        Option option = OptionFactory.getUfficioOption();
        if (stepOptions.hasOption(option.getLongOpt())){

            ufficio = commandLine.getOptionValue(option.getLongOpt());
            logger.debug(String.format("%s: %s", option.getDescription(), ufficio));
        }
    }
}
