package org.kafka.grep.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.kafka.grep.grepper.FactFinder;
import org.kafka.grep.grepper.PathFinder;
import org.kafka.grep.grepper.SQLParser;
import org.kafka.grep.transformer.PredicateTransformer;
import kafka.cluster.Broker;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.calcite.sql.SqlSelect;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class KafkaSql {
    private final ArgumentParser parser;

    public KafkaSql() {
        parser = ArgumentParsers.newArgumentParser("kafka")
                .defaultHelp(true)
                .description("Calculate checksum of given files.");
        parser.addArgument("--query")
                .dest("query")
                .type(String.class)
                .required(true)
                .help("sql query on kafka");

        parser.addArgument("--limit")
                .dest("limit")
                .type(Integer.class)
                .required(true)
                .help("whether to continue on match");

        parser.addArgument("--start")
                .dest("start")
                .type(String.class)
                .required(false)
                .help("start date to start search at");

        parser.addArgument("--end")
                .dest("end")
                .type(String.class)
                .required(false)
                .help("end date to end search at");

        parser.addArgument("--startEpoch")
                .dest("startEpoch")
                .type(Long.class)
                .required(false)
                .help("start date to start search at");

        parser.addArgument("--endEpoch")
                .dest("endEpoch")
                .type(Long.class)
                .required(false)
                .help("end date to end search at");


        parser.addArgument("--key")
                .dest("key")
                .type(Boolean.class)
                .required(false)
                .setDefault(false)
                .help("If the where clause has the key which is the same as the kafka partitioning key.");


        parser.addArgument("--skip_entity")
                .dest("skip_entity")
                .type(Boolean.class)
                .required(false)
                .setDefault(true)
                .help("when to skip the entity root.");


        parser.addArgument("--brokers")
                .dest("brokers")
                .type(String.class)
                .required(true)
                .help("Kafka brokers ");
    }

    Supplier<IllegalArgumentException> illegalInput = () -> new IllegalArgumentException();

    public void run(String args[]) throws Exception {
        Namespace namespace = parser.parseArgs(args);
        String brokers = namespace.getString("brokers");
        List<Broker> brokerList = KafkaZkUtils.deserializeBrokerList(brokers);
        String query = namespace.getString("query");
        SQLParser parser = new SQLParser();
        SqlSelect sqlSelect = parser.getSqlSelect(query).orElseThrow(illegalInput);
        String topic = parser.from(sqlSelect)[0];
        List<String> selectors = parser.selectors(sqlSelect);
        PredicateTransformer sqlToPredicateTranslator = new PredicateTransformer();
        Predicate predicate = sqlToPredicateTranslator.conditionalClause(sqlSelect);
        ObjectMapper mapper = new ObjectMapper();
        Function<JsonNode, Map<String, String>> extractor = (JsonNode o) -> {
            Map<String, String> map = new HashMap<>();
            if (selectors.stream().filter(x -> x.equals("*")).count() != 0) {
                try {
                    map.put("payload", mapper.writeValueAsString(o));
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                    map.put("payload", o.toString());
                }
            } else {
                selectors.stream().forEach(s -> {
                    List<String> list = new PathFinder().getValue(s).apply(o);
                    String val = null;
                    if (list != null && list.size() > 0) {
                        val = list.stream().collect(Collectors.joining(","));
                    }
                    map.put(s, val);
                });
            }
            return map;
        };
        boolean distinct = parser.distinct(sqlSelect);
        int limit = namespace.getInt("limit");
        Boolean isKey = namespace.getBoolean("key");
        Boolean skip_entity = namespace.getBoolean("skip_entity");
        if (namespace.getLong("startEpoch") != null && namespace.getLong("endEpoch") != null) {
            new KafkaReader(topic, brokerList, namespace.getLong("startEpoch"), namespace.getLong("endEpoch"), JsonNode.class, limit, skip_entity)
                    .start(new FactFinder<>(predicate, extractor, new Renderer(distinct), isKey));
        } else {
            new KafkaReader(topic, brokerList, namespace.getString("start"), namespace.getString("end"), JsonNode.class, limit, skip_entity)
                    .start(new FactFinder<>(predicate, extractor, new Renderer(distinct), isKey));
        }
    }
}
