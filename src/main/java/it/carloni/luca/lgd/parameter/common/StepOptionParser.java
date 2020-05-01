package it.carloni.luca.lgd.parameter.common;

import it.carloni.luca.lgd.option.OptionFactory;
import it.carloni.luca.lgd.parameter.step.DataDaDataAValues;
import it.carloni.luca.lgd.parameter.step.StepNameValue;
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
        String dataDaOptionValue = parseOptionOfTypeString(commandLine, OptionFactory.getDataDaOption());
        String dataAOptionValue = parseOptionOfTypeString(commandLine, OptionFactory.getDataAOpton());
        logger.info("Step options parsed correctly");
        return new DataDaDataAValues(dataDaOptionValue, dataAOptionValue);
    }
}
