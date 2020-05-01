package it.carloni.luca.lgd.spark.common;

public interface StepInterface<T> {

    void run(T t);
}
