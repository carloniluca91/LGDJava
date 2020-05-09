package it.carloni.luca.lgd;

import it.carloni.luca.lgd.option.*;
import it.carloni.luca.lgd.parameter.common.StepOptionParser;
import it.carloni.luca.lgd.parameter.step.*;
import it.carloni.luca.lgd.spark.step.*;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class StepRunner {

    private final Logger logger = Logger.getLogger(getClass());
    private final StepOptionParser stepOptionParser = new StepOptionParser();

    public void run(String[] args, String stepName) {

        Options stepParameterOptions = new Options();

        try {

            String stepnameUC = stepName.toUpperCase();
            StepName stepNameElem = StepName.valueOf(stepnameUC);
            switch (stepNameElem) {

                case CICLILAV_STEP_1: {

                    logger.info("Matched step name " + stepnameUC);

                    Option dataDaOption = OptionFactory.getDataDaOption();
                    Option dataAOption = OptionFactory.getDataAOpton();

                    stepParameterOptions.addOption(dataDaOption);
                    stepParameterOptions.addOption(dataAOption);

                    DataDaDataAValues dataDaDataAValues = stepOptionParser.getDataDaDataAValues(args, stepParameterOptions);
                    new CiclilavStep1().run(dataDaDataAValues);
                    break;
                }

                case CICLI_PREVIEW: {

                    logger.info("Matched step name " + stepnameUC);

                    Option dataAOption = OptionFactory.getDataAOpton();
                    Option ufficioOption = OptionFactory.getUfficioOption();

                    stepParameterOptions.addOption(dataAOption);
                    stepParameterOptions.addOption(ufficioOption);

                    DataAUfficioValues dataAUfficioValues = stepOptionParser.getDataAUfficioValues(args, stepParameterOptions);
                    new CicliPreview().run(dataAUfficioValues);
                    break;
                }

                case FANAG_MONTHLY: {

                    logger.info("Matched step name " + stepnameUC);

                    Option dataAOption = OptionFactory.getDataAOpton();
                    Option numeroMesi1Option = OptionFactory.getNumeroMesi1Option();
                    Option numeroMesi2Option = OptionFactory.getNumeroMesi2Option();

                    stepParameterOptions.addOption(dataAOption);
                    stepParameterOptions.addOption(numeroMesi1Option);
                    stepParameterOptions.addOption(numeroMesi2Option);

                    DataANumeroMesi12Values dataANumeroMesi12Values = stepOptionParser.getDataANumeroMesi12Values(args, stepParameterOptions);
                    new FanagMonthly().run(dataANumeroMesi12Values);
                    break;
                }

                case FPASPERD:

                    logger.info("Matched step name " + stepnameUC);

                    new Fpasperd().run(new EmptyValues());

                default:

                    logger.error(String.format("Undefined step name (%s)", stepName));
                    break;
            }
        }

        catch (IllegalArgumentException e) {

            logger.error("IllegalArgumentException occurred");
            logger.error(String.format("Unable to match provided step name %s", stepName));
        }

        catch (ParseException e) {

            // IF THE PROVIDED STEP PARAMETERS ARE INCORRECT
            logger.error("ParseException occurred");
            logger.error(e);

            HelpFormatter helpFormatter = new HelpFormatter();
            String helpUsageString = OptionEnum.HELP_USAGE_STRING + " -" + OptionEnum.STEP_NAME_SHORT_OPTION + " " + stepName;
            String helpHeaderString = OptionEnum.HELP_HEADER_STRING + ": step " + stepName + "\n\n";
            String helpFooterString = OptionEnum.HELP_FOOTER_STRING;
            helpFormatter.setWidth(getHelpFormatterWidth(helpUsageString, helpHeaderString, helpFooterString));
            helpFormatter.printHelp(helpUsageString, helpHeaderString, stepParameterOptions, helpFooterString, true);
        }
    }

    private int getHelpFormatterWidth(String usageString, String headerString, String footerString) {

        String fullUsageString = "usage: " + usageString;
        List<String> helpFormmatterStrings = Arrays.asList(fullUsageString, headerString, footerString);
        List<Integer> helpFormatterStringsLength = helpFormmatterStrings
                .stream()
                .map((String::length))
                .collect(Collectors.toList());

        return Collections.max(helpFormatterStringsLength);
    }
}
