package de.bwaldvogel.mongo.backend;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.entry;

import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.Test;

public class StreamUtilsTest {

    @Test
    public void testToLinkedHashMap() throws Exception {
        Map<String, String> result = Stream.of("a", "b", "c")
            .map(value -> new SimpleEntry<>(value, value))
            .collect(StreamUtils.toLinkedHashMap());
        assertThat(result)
            .containsExactly(
                entry("a", "a"),
                entry("b", "b"),
                entry("c", "c")
            );
    }

    @Test
    public void testToLinkedHashMap_duplicates() throws Exception {
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> Stream.of("a", "b", "c", "b")
                .map(value -> new SimpleEntry<>(value, value))
                .collect(StreamUtils.toLinkedHashMap()))
            .withMessage("Duplicate key 'b'");
    }

    @Test
    public void testToLinkedHashSet() throws Exception {
        Set<String> result = Stream.of("a", "b", "c", "b")
            .collect(StreamUtils.toLinkedHashSet());
        assertThat(result).containsExactly("a", "b", "c");
    }

}
