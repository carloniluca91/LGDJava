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
        Options firstStageOptions = new Options();
        firstStageOptions.addOption(stepNameOption);

        StepOptionParser stepOptionParser = new StepOptionParser();

        try {

            String stepName = stepOptionParser.buildStepNameValue(args, firstStageOptions).getStepName();
            new StepRunner().run(args, stepName);

        } catch (ParseException e) {

            // IF NO STEP NAME HAS BEEN PROVIDED
            logger.error("ParseException (related to step name option) occurred");
            logger.error(e);

            HelpFormatter helpFormatter = new HelpFormatter();
            String helpUsageString = OptionEnum.HELP_USAGE_STRING.getString();
            String helpHeaderString = OptionEnum.HELP_HEADER_STRING.getString() + "\n\n";
            String helpFooterString = OptionEnum.HELP_FOOTER_STRING.getString();

            helpFormatter.setWidth(getHelpFormatterWidth(helpHeaderString, helpFooterString));
            helpFormatter.printHelp(helpUsageString, helpHeaderString, firstStageOptions, helpFooterString, true);
        }
    }

    private static int getHelpFormatterWidth(String headerString, String footerString) {

        String usageString = "usage: "
                + OptionEnum.HELP_USAGE_STRING.getString()
                + " -"
                + OptionEnum.STEP_NAME_SHORT_OPTION.getString()
                + " <arg>";

        List<String> helpFormmatterStrings = Arrays.asList(usageString, headerString, footerString);
        List<Integer> helpFormatterStringsLength = helpFormmatterStrings
                .stream()
                .map((String::length))
                .collect(Collectors.toList());

        return Collections.max(helpFormatterStringsLength);
    }
}
