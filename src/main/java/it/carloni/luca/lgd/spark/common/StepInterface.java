package it.carloni.luca.lgd.spark.common;

import it.carloni.luca.lgd.parameter.common.AbstractStepValue;

public interface StepInterface<T extends AbstractStepValue> {

    void run(T t);
}
