package de.bwaldvogel.mongo.backend;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

import de.bwaldvogel.mongo.backend.memory.index.MemoryUniqueIndex;
import de.bwaldvogel.mongo.bson.Document;

public class IndexTest {

    @Test
    public void testGetKeyValue_simple() throws Exception {
        Index<?> index = new MemoryUniqueIndex(Collections.singletonList(new IndexKey("x", true)), false);

        assertThat(index.getKeyValue(new Document("x", 1))).containsExactly(1.0);
        assertThat(index.getKeyValue(new Document("x", "abc"))).containsExactly("abc");
    }

    @Test
    public void testGetKeyValue_compound() throws Exception {
        Index<?> index = new MemoryUniqueIndex(Arrays.asList(
            new IndexKey("a", true),
            new IndexKey("b", true)), false);

        assertThat(index.getKeyValue(new Document("a", 1).append("b", 2))).containsExactly(1.0, 2.0);
        assertThat(index.getKeyValue(new Document("a", "x").append("b", "y"))).containsExactly("x", "y");
        assertThat(index.getKeyValue(new Document("b", "y").append("a", "x"))).containsExactly("x", "y");
    }

    @Test
    public void testGetKeyValue_Subdocument() throws Exception {
        Index<?> index = new MemoryUniqueIndex(Collections.singletonList(new IndexKey("a.b", true)), false);

        assertThat(index.getKeyValue(new Document("a", 1))).isEqualTo(Collections.singletonList(null));
        assertThat(index.getKeyValue(new Document("a", new Document("b", 1)))).containsExactly(1.0);
    }

}
