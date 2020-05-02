package it.carloni.luca.lgd.spark.step;

import lombok.Getter;

public enum StepName {

    CICLILAV_STEP_1("CICLILAV_STEP_1"),
    CICLI_PREVIEW("CICLI_PREVIEW");

    @Getter private String stepName;

    StepName(String value) {
        this.stepName = value;
    }

}
