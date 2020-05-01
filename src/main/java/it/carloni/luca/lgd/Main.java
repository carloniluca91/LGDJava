package it.carloni.luca.lgd;

import it.carloni.luca.lgd.option.OptionEnum;
import it.carloni.luca.lgd.option.OptionFactory;
import it.carloni.luca.lgd.parameter.common.StepOptionParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class Main {

    public static void main(String[] args) {

        Logger logger = Logger.getRootLogger();

        Option stepNameOption = OptionFactory.getStepNameOption();
        Options stepNameOptions = new Options();
        stepNameOptions.addOption(stepNameOption);

        StepOptionParser stepOptionParser = new StepOptionParser();

        try {

            String stepName = stepOptionParser.getStepNameValue(args, stepNameOptions).getStepName();
            new StepRunner().run(args, stepName);

        } catch (ParseException e) {

            // IF NO STEP NAME HAS BEEN PROVIDED
            logger.error("ParseException related to step name occurred");
            logger.error(e);

            HelpFormatter helpFormatter = new HelpFormatter();
            String helpUsageString = OptionEnum.HELP_USAGE_STRING;
            String helpHeaderString = OptionEnum.HELP_HEADER_STRING + "\n\n";
            String helpFooterString = OptionEnum.HELP_FOOTER_STRING;

            helpFormatter.setWidth(getHelpFormatterWidth(helpHeaderString, helpFooterString));
            helpFormatter.printHelp(helpUsageString, helpHeaderString, stepNameOptions, helpFooterString, true);
        }
    }

    private static int getHelpFormatterWidth(String headerString, String footerString) {

        String usageString =("usage: " + OptionEnum.HELP_USAGE_STRING + " -" + OptionEnum.STEP_NAME_SHORT_OPTION + " <arg>");
        List<String> helpFormmatterStrings = Arrays.asList(usageString, headerString, footerString);
        List<Integer> helpFormatterStringsLength = helpFormmatterStrings
                .stream()
                .map((String::length))
                .collect(Collectors.toList());

        return Collections.max(helpFormatterStringsLength);
    }
}
