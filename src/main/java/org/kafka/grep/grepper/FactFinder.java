package org.kafka.grep.grepper;


import org.kafka.grep.kafka.MessageHandler;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public class FactFinder<T, R> implements MessageHandler<T> {

    private final Predicate<T> p;
    private final Function<T, R> extractor;
    private Consumer<R> renderer;
    private final boolean isPartitionAware;
    private volatile String persuer = null;

    public FactFinder(Predicate<T> p, Function<T, R> extractor, Consumer<R> renderer, boolean isPartitionAware) {
        this.renderer = renderer;
        this.isPartitionAware = isPartitionAware;
        if (p == null) {
            p = x -> true;
        }
        this.p = p;
        this.extractor = extractor;
    }


    @Override
    public boolean consumeMessage(T message) {
        if (isPartitionAware) {
            if (persuer != null && !Thread.currentThread().getName().equals(persuer)) {
                return false;
            }
        }
        if (p.test(message)) {
            if (isPartitionAware && persuer == null) {
                persuer = Thread.currentThread().getName();
            }
            extract(message);
            return true;
        }
        return false;
    }

    private void extract(T message) {
        renderer.accept(extractor.apply(message));
    }

    @Override
    public void partialUpdate() {
    }
}
