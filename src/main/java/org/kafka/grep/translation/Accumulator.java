package org.kafka.grep.translation;

public interface Accumulator<T> {


    void mergeAND(T x);

    void mergeOR(T x);

    void mergeCondition(T x);

    T get();
}
