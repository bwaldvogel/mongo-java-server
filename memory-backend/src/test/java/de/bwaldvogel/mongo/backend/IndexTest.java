package de.bwaldvogel.mongo.backend;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.Arrays;
import java.util.Collections;

import org.junit.jupiter.api.Test;

import de.bwaldvogel.mongo.backend.memory.index.MemoryUniqueIndex;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.CannotIndexParallelArraysError;

public class IndexTest {

    @Test
    void testGetKeyValues_simple() throws Exception {
        Index<?> index = new MemoryUniqueIndex("index", Collections.singletonList(new IndexKey("x", true)), false);

        assertThat(index.getKeyValues(new Document("x", 1))).containsExactly(new KeyValue(1.0));
        assertThat(index.getKeyValues(new Document("x", "abc"))).containsExactly(new KeyValue("abc"));
    }

    @Test
    void testGetKeyValues_compound() throws Exception {
        Index<?> index = new MemoryUniqueIndex("index", Arrays.asList(
            new IndexKey("a", true),
            new IndexKey("b", true)), false);

        assertThat(index.getKeyValues(new Document("a", 1).append("b", 2))).containsExactly(new KeyValue(1.0, 2.0));
        assertThat(index.getKeyValues(new Document("a", "x").append("b", "y"))).containsExactly(new KeyValue("x", "y"));
        assertThat(index.getKeyValues(new Document("b", "y").append("a", "x"))).containsExactly(new KeyValue("x", "y"));
    }

    @Test
    void testGetKeyValues_Subdocument() throws Exception {
        Index<?> index = new MemoryUniqueIndex("index", Collections.singletonList(new IndexKey("a.b", true)), false);

        assertThat(index.getKeyValues(new Document("a", 1))).containsExactly(new KeyValue((Object) null));
        assertThat(index.getKeyValues(new Document("a", new Document("b", 1)))).containsExactly(new KeyValue(1.0));
        assertThat(index.getKeyValues(new Document("a", 1))).containsExactly(new KeyValue((Object) null));
        assertThat(index.getKeyValues(new Document("a", new Document("b", 1)))).containsExactly(new KeyValue(1.0));
    }

    @Test
    void testGetKeyValues_multiKey() throws Exception {
        Index<?> index = new MemoryUniqueIndex("index", Collections.singletonList(new IndexKey("a", true)), false);

        assertThat(index.getKeyValues(new Document("a", Arrays.asList(1, 2, 3, 2))))
            .containsExactly(
                new KeyValue(1.0),
                new KeyValue(2.0),
                new KeyValue(3.0));
    }

    @Test
    void testGetKeyValues_compoundMultiKey() throws Exception {
        Index<?> index = new MemoryUniqueIndex("index", Arrays.asList(
            new IndexKey("a", true),
            new IndexKey("b", true),
            new IndexKey("c", true)
        ), false);

        assertThat(index.getKeyValues(new Document("b", Arrays.asList(1, 2, 3)).append("c", 20).append("a", "value")))
            .containsExactly(
                new KeyValue("value", 1.0, 20.0),
                new KeyValue("value", 2.0, 20.0),
                new KeyValue("value", 3.0, 20.0));
    }

    @Test
    void testGetKeyValues_compoundMultiKey_tooManyCollections() throws Exception {
        Index<?> index = new MemoryUniqueIndex("index", Arrays.asList(
            new IndexKey("a", true),
            new IndexKey("b", true)
        ), false);

        assertThatExceptionOfType(CannotIndexParallelArraysError.class)
            .isThrownBy(() -> index.getKeyValues(new Document("a", Arrays.asList(1, 2, 3)).append("b", Arrays.asList("x", "y"))))
            .withMessage("[Error 171] cannot index parallel arrays [b] [a]");
    }

    @Test
    void testGetKeyValues_multiKey_document() throws Exception {
        Index<?> index = new MemoryUniqueIndex("index", Collections.singletonList(new IndexKey("a.b", true)), false);

        assertThat(index.getKeyValues(new Document("a", Arrays.asList(new Document("b", 1), new Document("b", 2)))))
            .containsExactly(
                new KeyValue(1.0),
                new KeyValue(2.0));

        assertThat(index.getKeyValues(new Document("a", Arrays.asList(new Document("c", 1), new Document("b", 2)))))
            .containsExactly(
                new KeyValue((Object) null),
                new KeyValue(2.0));
    }

}
