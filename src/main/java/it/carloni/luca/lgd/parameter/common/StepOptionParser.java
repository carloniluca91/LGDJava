package it.carloni.luca.lgd.parameter.common;

import it.carloni.luca.lgd.option.OptionFactory;
import it.carloni.luca.lgd.parameter.step.*;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

public class StepOptionParser {

    private final Logger logger = Logger.getLogger(getClass());
    protected final RelaxedParser relaxedParser = new RelaxedParser();

    private Integer parseOptionOfTypeInt(CommandLine commandLine, Option optionToParse) {

        if (commandLine.hasOption(optionToParse.getLongOpt())) {

            int optionValue = Integer.parseInt(commandLine.getOptionValue(optionToParse.getLongOpt()));
            logger.info(String.format("%s: %s", optionToParse.getDescription(), optionValue));
            return optionValue;
        }

        else return null;
    }

    private String parseOptionOfTypeString(CommandLine commandLine, Option optionToParse) {

        if (commandLine.hasOption(optionToParse.getLongOpt())){

            String optionValue = commandLine.getOptionValue(optionToParse.getLongOpt());
            logger.info(String.format("%s: %s", optionToParse.getDescription(), optionValue));
            return optionValue;
        }

        else return null;
    }

    private CommandLine getCommandLine(String[] args, Options stepOptions) throws ParseException {

        return relaxedParser.parse(stepOptions, args);
    }

    public StepNameValue getStepNameValue(String[] args, Options stepOptions) throws ParseException {

        CommandLine commandLine = getCommandLine(args, stepOptions);
        String stepName = parseOptionOfTypeString(commandLine, OptionFactory.getStepNameOption());

        logger.info("Step name parsed correctly");

        return new StepNameValue(stepName);
    }

    public DataDaDataAValues getDataDaDataAValues(String[] args, Options stepOptions) throws ParseException {

        CommandLine commandLine = getCommandLine(args, stepOptions);
        String dataDa = parseOptionOfTypeString(commandLine, OptionFactory.getDataDaOption());
        String dataA = parseOptionOfTypeString(commandLine, OptionFactory.getDataAOpton());

        logger.info("Step options parsed correctly");

        return new DataDaDataAValues(dataDa, dataA);
    }

    public DataAUfficioValues getDataAUfficioValues(String[] args, Options stepOptions) throws ParseException {

        CommandLine commandLine = getCommandLine(args, stepOptions);
        String dataA = parseOptionOfTypeString(commandLine, OptionFactory.getDataAOpton());
        String ufficio = parseOptionOfTypeString(commandLine, OptionFactory.getUfficioOption());

        logger.info("Step options parsed correctly");

        return new DataAUfficioValues(dataA, ufficio);
    }

    public DataANumeroMesi12Values getDataANumeroMesi12Values(String[] args, Options stepOptions) throws ParseException {

        CommandLine commandLine = getCommandLine(args, stepOptions);
        String dataA = parseOptionOfTypeString(commandLine, OptionFactory.getDataAOpton());
        Integer numeroMesi1 = parseOptionOfTypeInt(commandLine, OptionFactory.getNumeroMesi1Option());
        Integer numeroMesi2 = parseOptionOfTypeInt(commandLine, OptionFactory.getNumeroMesi2Option());

        logger.info("Step options parsed correctly");

        return new DataANumeroMesi12Values(dataA, numeroMesi1, numeroMesi2);

    }
}
