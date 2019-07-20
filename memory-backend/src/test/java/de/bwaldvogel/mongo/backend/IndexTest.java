package de.bwaldvogel.mongo.backend;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

import de.bwaldvogel.mongo.backend.memory.index.MemoryUniqueIndex;
import de.bwaldvogel.mongo.bson.Document;

public class IndexTest {

    @Test
    public void testGetKeyValues_simple() throws Exception {
        Index<?> index = new MemoryUniqueIndex(Collections.singletonList(new IndexKey("x", true)), false);

        assertThat(index.getKeyValues(new Document("x", 1))).containsExactly(new KeyValue(1.0));
        assertThat(index.getKeyValues(new Document("x", "abc"))).containsExactly(new KeyValue("abc"));
    }

    @Test
    public void testGetKeyValues_compound() throws Exception {
        Index<?> index = new MemoryUniqueIndex(Arrays.asList(
            new IndexKey("a", true),
            new IndexKey("b", true)), false);

        assertThat(index.getKeyValues(new Document("a", 1).append("b", 2))).containsExactly(new KeyValue(1.0, 2.0));
        assertThat(index.getKeyValues(new Document("a", "x").append("b", "y"))).containsExactly(new KeyValue("x", "y"));
        assertThat(index.getKeyValues(new Document("b", "y").append("a", "x"))).containsExactly(new KeyValue("x", "y"));
    }

    @Test
    public void testGetKeyValues_Subdocument() throws Exception {
        Index<?> index = new MemoryUniqueIndex(Collections.singletonList(new IndexKey("a.b", true)), false);

        assertThat(index.getKeyValues(new Document("a", 1))).containsExactly(new KeyValue((Object) null));
        assertThat(index.getKeyValues(new Document("a", new Document("b", 1)))).containsExactly(new KeyValue(1.0));
        assertThat(index.getKeyValues(new Document("a", 1))).containsExactly(new KeyValue((Object) null));
        assertThat(index.getKeyValues(new Document("a", new Document("b", 1)))).containsExactly(new KeyValue(1.0));
    }

    @Test
    public void testGetKeyValues_ArrayValue() throws Exception {
        Index<?> index = new MemoryUniqueIndex(Collections.singletonList(new IndexKey("a", true)), false);

        assertThat(index.getKeyValues(new Document("a", Arrays.asList(1, 2, 3))))
            .containsExactly(
                new KeyValue(1.0),
                new KeyValue(2.0),
                new KeyValue(3.0));
    }

}
