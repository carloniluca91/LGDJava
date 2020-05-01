package it.carloni.luca.lgd.spark.common;

import it.carloni.luca.lgd.parameter.common.AbstractStepValues;

public interface StepInterface<T extends AbstractStepValues> {

    void run(T t);
}
