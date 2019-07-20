package de.bwaldvogel.mongo.backend;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BinaryOperator;
import java.util.stream.Collector;
import java.util.stream.Collectors;

final class StreamUtils {

    private StreamUtils() {
    }

    private static <T> BinaryOperator<T> throwingMerger() {
        return (u, v) -> {
            throw new IllegalStateException(String.format("Duplicate key %s", u));
        };
    }

    static <K, V> Collector<Entry<K, V>, ?, Map<K, V>> toLinkedHashMap() {
        return Collectors.toMap(Entry::getKey, Entry::getValue, throwingMerger(), LinkedHashMap::new);
    }
}
