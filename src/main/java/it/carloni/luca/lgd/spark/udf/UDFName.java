package it.carloni.luca.lgd.spark.udf;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public enum UDFName {

    ADD_DURATION("ADD_DURATION"),
    SUBTRACT_DURATION("SUBTRACT_DURATION"),
    CHANGE_DATE_FORMAT("CHANGE_DATE_FORMAT"),
    CHANGE_DATE_FORMAT_FROM_Y2_TO_Y4("CHANGE_DATE_FORMAT_FROM_Y2_TO_Y4"),
    DAYS_BETWEEN("DAYS_BETWEEN"),
    GREATEST_DATE("GREATEST_DATE"),
    LEAST_DATE("LEAST_DATE");

    @Getter private final String name;
}
