package steps.params;

import lombok.Getter;
import org.apache.commons.cli.*;
import org.apache.log4j.Logger;

import java.util.List;

public class StepParams {

    private Logger logger;

    @Getter private String dataDa;
    @Getter private String dataA;
    @Getter private String ufficio;
    @Getter private int numeroMesi1;
    @Getter private int numeroMesi2;
    @Getter private String dataOsservazione;

    private List<Option> optionList;
    private Options stepParamsOptions;
    private CommandLine commandLine;

    public StepParams(String[] args, List<Option> optionList){

        logger = Logger.getLogger(getClass());
        this.optionList = optionList;
        stepParamsOptions = new Options();

        for (Option option: optionList){

            option.setRequired(true);
            stepParamsOptions.addOption(option);
        }

        parseArgs(args);
    }

    private void parseArgs(String[] args){

        try {

            CommandLineParser commandLineParser = new BasicParser();
            commandLine = commandLineParser.parse(stepParamsOptions, args);

            setDataDa();
            setDataA();
            setUfficio();
            setNumeroMesi1();
            setNumeroMesi2();
            setDataOsservazione();

            logger.info("Arguments parsed correctly");

        } catch (ParseException e) {

            logger.error("ParseException occurred");
            logger.error("e.getMessage(): " + e.getMessage());
            logger.error(e);
        }
    }

    private void setDataA(){

        Option option = OptionFactory.getDataAOpton();
        if (optionList.contains(option)){

            dataA = commandLine.getOptionValue(option.getLongOpt());
            logger.debug(String.format("%s: %s", option.getDescription(), dataA));
        }
    }

    private void setDataDa(){

        Option option = OptionFactory.getDataDaOption();
        if (optionList.contains(option)){

            dataDa = commandLine.getOptionValue(option.getLongOpt());
            logger.debug(String.format("%s: %s", option.getDescription(), dataDa));
        }
    }

    private void setDataOsservazione(){

        Option option = OptionFactory.getDataOsservazioneOption();
        if (optionList.contains(option)){

            dataOsservazione = commandLine.getOptionValue(option.getLongOpt());
            logger.debug(String.format("%s: %s", option.getDescription(), dataOsservazione));
        }
    }

    private void setNumeroMesi1(){

        Option option = OptionFactory.getNumeroMesi1Option();
        if (optionList.contains(option)){

            numeroMesi1 = Integer.parseInt(commandLine.getOptionValue(option.getLongOpt()));
            logger.debug(String.format("%s: %s", option.getDescription(), numeroMesi1));
        }
    }

    private void setNumeroMesi2(){

        Option option = OptionFactory.getNumeroMesi2Option();
        if (optionList.contains(option)){

            numeroMesi2 = Integer.parseInt(commandLine.getOptionValue(option.getLongOpt()));
            logger.debug(String.format("%s: %s", option.getDescription(), numeroMesi2));
        }
    }

    private void setUfficio(){

        Option option = OptionFactory.getUfficioOption();
        if (optionList.contains(option)){

            ufficio = commandLine.getOptionValue(option.getLongOpt());
            logger.debug(String.format("%s: %s", option.getDescription(), ufficio));
        }
    }
}
