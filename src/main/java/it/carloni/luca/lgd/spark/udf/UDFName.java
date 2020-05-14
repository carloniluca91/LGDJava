package it.carloni.luca.lgd.spark.udf;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public enum UDFName {

    ADD_DURATION("ADD_DURATION"),
    SUBTRACT_DURATION("SUBTRACT_DURATION"),
    CHANGE_DATE_FORMAT("CHANGE_DATE_FORMAT"),
    DAYS_BETWEEN("DAYS_BETWEEN"),
    GREATEST_DATE("GREATEST_DATE"),
    LEAST_DATE("LEAST_DATE"),
    IS_DATE_BETWEEN("IS_DATE_BETWEEN");

    @Getter private final String name;
}
