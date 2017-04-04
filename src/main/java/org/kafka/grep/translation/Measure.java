package org.kafka.grep.translation;

import java.util.Objects;

public class Measure {

    private String name;
    private String aggregate;

    public Measure(String name, String aggregate) {
        this.name = name;
        this.aggregate = aggregate;
    }

    public String getName() {
        return name;
    }

    public String getAggregate() {
        return aggregate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Measure measure = (Measure) o;
        return Objects.equals(name, measure.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
