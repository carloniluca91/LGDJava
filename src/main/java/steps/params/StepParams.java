package steps.params;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.List;

public class StepParams {

    private Logger logger = Logger.getLogger(this.getClass().getName());

    private String dataDa;
    private String dataA;
    private String ufficio;
    private int numeroMesi1;
    private int numeroMesi2;

    private List<Option> optionList;
    private Options stepParamsOptions;
    private CommandLine commandLine;

    public StepParams(String[] args, Option ... options){

        optionList = Arrays.asList(options);
        stepParamsOptions = new Options();

        for (Option option: options){

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

            logger.info("Arguments parsed correctly");

        } catch (ParseException e) {

            logger.error(e.getMessage());
        }
    }

    public String getDataDa() {
        return dataDa;
    }

    public String getDataA() {
        return dataA;
    }

    public int getNumeroMesi1() {
        return numeroMesi1;
    }

    public int getNumeroMesi2() {
        return numeroMesi2;
    }

    public String getUfficio() {
        return ufficio;
    }

    private void setDataDa(){

        Option option = OptionFactory.getDataDaOption();
        if (optionList.contains(option)){

            dataDa = commandLine.getOptionValue(option.getLongOpt());
            logger.debug(String.format("%s: %s", option.getDescription(), dataDa));
        }
    }

    private void setDataA(){

        Option option = OptionFactory.getDataAOpton();
        if (optionList.contains(option)){

            dataA = commandLine.getOptionValue(option.getLongOpt());
            logger.debug(String.format("%s: %s", option.getDescription(), dataA));
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
