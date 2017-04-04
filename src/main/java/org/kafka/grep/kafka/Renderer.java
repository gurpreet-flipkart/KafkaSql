package org.kafka.grep.kafka;


import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Consumer;

import static org.kafka.grep.kafka.ThreadPrinter.THREADPRINT;

public class Renderer implements Consumer<Map<String, String>> {

    private final boolean unique;

    public Renderer(boolean unique) {
        this.unique = unique;
    }

    private Set<String> treeSet = new ConcurrentSkipListSet<>();

    @Override
    public void accept(Map<String, String> properties) {
        if (unique) {
            String key = properties.values().stream().reduce("", (a, b) -> a + "-" + b);
            if (!treeSet.contains(key)) {
                treeSet.add(key);
                print(properties);
            }
        } else {
            print(properties);
        }
    }


    private void print(Map<String, String> properties) {
        for (String s : properties.keySet()) {
            THREADPRINT.print(s + ":" + properties.get(s) + "\t");
        }
        System.out.println();
    }
}
