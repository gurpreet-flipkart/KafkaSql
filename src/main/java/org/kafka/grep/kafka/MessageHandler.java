package org.kafka.grep.kafka;

public interface MessageHandler<T> {
    boolean consumeMessage(T message);
    void partialUpdate();
}
