package org.kafka.grep.translation;

import java.util.function.Predicate;

public class PredicateAccumulator implements Accumulator<Predicate> {
    Predicate predicate = null;

    @Override
    public void mergeAND(Predicate x) {
        if (predicate == null) {
            predicate = x;
        } else {
            predicate = predicate.and(x);
        }
    }

    @Override
    public void mergeOR(Predicate x) {
        if (predicate == null) {
            predicate = x;
        } else {
            predicate = predicate.or(x);
        }
    }

    @Override
    public void mergeCondition(Predicate x) {
        predicate = x;
    }

    @Override
    public Predicate get() {
        return predicate;
    }
}
