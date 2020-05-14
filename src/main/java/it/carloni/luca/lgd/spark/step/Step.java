package it.carloni.luca.lgd.spark.step;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public enum Step {

    CICLILAV_STEP_1("CICLILAV_STEP_1"),
    CICLI_PREVIEW("CICLI_PREVIEW"),
    FANAG_MONTHLY("FANAG_MONTHLY"),
    FPASPERD("FPASPERD"),
    FRAPP_NDG_MONTHLY("FRAPP_NDG_MONTHLY"),
    FRAPP_PUMA("FRAPP_PUMA"),
    MOVIMENTI("MOVIMENTI"),
    POSAGGR("POSAGGR"),
    QUAD_FCOLL("QUAD_FCOLL"),
    QUAD_FCOLL_CICLI("QUAD_FCOLL_CICLI"),
    QUAD_FPOSI("QUAD_FPOSI"),
    QUAD_FRAPP("QUAD_FRAPP"),
    RACC_INC("RACC_INC"),
    SOFFERENZE_PREVIEW("SOFFERENZE_PREVIEW");

    @Getter private String name;

}
