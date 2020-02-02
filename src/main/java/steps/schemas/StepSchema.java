package steps.schemas;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StepSchema {

    public static Map<String, String> fromStreamToMap(Stream<String[]> stream) {

        return stream.collect(Collectors.collectingAndThen(
                Collectors.toMap(data -> data[0], data -> data[1]),
                Collections::unmodifiableMap));
    }
}
