package it.carloni.luca.lgd.spark.step;

import lombok.Getter;

public enum StepName {

    CICLILAV_STEP_1("CICLILAV_STEP_1"),
    CICLI_PREVIEW("CICLI_PREVIEW"),
    FANAG_MONTHLY("FANAG_MONTHLY"),
    FPASPERD("FPASPERD"),
    FRAPP_NDG_MONTHLY("FRAPP_NDG_MONTHLY"),
    FRAPP_PUMA("FRAPP_PUMA"),
    MOVIMENTI("MOVIMENTI");

    @Getter private String stepName;

    StepName(String value) {
        this.stepName = value;
    }

}
