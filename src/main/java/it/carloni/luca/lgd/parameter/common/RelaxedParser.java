package it.carloni.luca.lgd.parameter.common;

import org.apache.commons.cli.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class RelaxedParser extends BasicParser {

    @Override
    public CommandLine parse(Options options, String[] arguments) throws ParseException {

        List<String> knownArguments = new ArrayList<>();
        Iterator<String> argsIterator = Arrays.stream(arguments).iterator();

        while (argsIterator.hasNext()) {

            String currentArg = argsIterator.next();
            if (options.hasOption(currentArg)) {

                // ADD THE CURRENT STRING (WHICH REPRESENTS THE OPTION) TO THE LIST OF ARGUMENTS TO BE CONSIDERED
                // IF THE OPTION REQUIRES AN ARGUMENT, THE ARGUMENT (WHICH IS THE NEXT STRING) MUST ALSO BE ADDED
                knownArguments.add(currentArg);
                Option currentArgOption = options.getOption(currentArg);
                if (currentArgOption.hasArg()) {

                    knownArguments.add(argsIterator.next());
                }
            }
        }

        return super.parse(options, knownArguments.toArray(new String[0]));
    }
}
