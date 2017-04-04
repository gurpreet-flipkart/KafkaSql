package org.kafka.grep;

import org.kafka.grep.kafka.KafkaSql;

public class KafkaSqlApplication{

    public static void main(final String[] args) throws Exception {
        new KafkaSql().run(args);
    }

}
