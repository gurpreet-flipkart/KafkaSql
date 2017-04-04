package org.kafka.grep.grepper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

public class PathFinder {
    public Function<JsonNode, List<String>> getValue(String path) {
        String splitPath[] = path.split("\\.");
        return new PathGetter(Arrays.asList(splitPath));
    }

    public Predicate<JsonNode> identifierMatchPredicate(String path1, String path2, BiFunction<String, String, Boolean> comparator) {
        String splitPath1[] = path1.split("\\.");
        String splitPath2[] = path2.split("\\.");
        return (JsonNode o) -> {
            PathGetter pathGetter1 = new PathGetter(Arrays.asList(splitPath1));
            PathGetter pathGetter2 = new PathGetter(Arrays.asList(splitPath2));
            return comparator.apply(pathGetter1.apply(o).get(0), pathGetter2.apply(o).get(0));
        };
    }

    public Predicate<JsonNode> literalMatchPredicate(String path, String match, BiFunction<String, String, Boolean> comparator) {
        String splitPath[] = path.split("\\.");
        return (JsonNode o) -> {
            List<String> matches = new PathGetter(Arrays.asList(splitPath)).apply(o);
            Optional<String> firstMatch = matches.stream().filter(m -> comparator.apply(m, match)).findFirst();
            return firstMatch.isPresent();
        };
    }

    private class PathGetter implements Function<JsonNode, List<String>> {
        private List<String> path;

        PathGetter(List<String> path) {
            this.path = path;
        }

        @Override
        public List<String> apply(JsonNode object) {
            if (!path.isEmpty()) {
                String s = path.get(0);
                JsonNode field = object.get(s);
                if (field == null) {
                    return ImmutableList.of("null");
                }
                if (field.getNodeType() == JsonNodeType.ARRAY) {
                    if (field.get(0) == null) {
                        return ImmutableList.of("null");
                    }
                    ArrayList<String> values = new ArrayList<>();
                    List<String> remainingList = path.subList(1, path.size());
                    if (remainingList.isEmpty()) {
                        for (JsonNode j : field) {
                            values.add(j.asText());
                        }
                    } else {
                        for (JsonNode j : field) {
                            values.addAll(new PathGetter(remainingList).apply(j));
                        }
                    }
                    return values;
                } else {
                    List<String> remainingList = path.subList(1, path.size());
                    if (remainingList.isEmpty()) {
                        return ImmutableList.of(field.asText());
                    }
                    return new PathGetter(remainingList).apply(field);
                }
            }
            return null;
        }
    }

}
