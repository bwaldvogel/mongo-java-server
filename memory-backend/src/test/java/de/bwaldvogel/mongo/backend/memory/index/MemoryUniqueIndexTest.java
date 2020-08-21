package de.bwaldvogel.mongo.backend.memory.index;

import static de.bwaldvogel.mongo.backend.TestUtils.json;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.Arrays;
import java.util.Map.Entry;

import org.junit.jupiter.api.Test;

import de.bwaldvogel.mongo.backend.Index;
import de.bwaldvogel.mongo.backend.IndexKey;
import de.bwaldvogel.mongo.backend.KeyValue;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.CannotIndexParallelArraysError;

public class MemoryUniqueIndexTest {

    @Test
    void testGetKeyValues_multiKey_simpleCase() throws Exception {
        Index<?> index = new MemoryUniqueIndex("name", Arrays.asList(
            new IndexKey("a", true),
            new IndexKey("b", true)
        ), false);

        assertThat(index.getKeyValues(new Document(json("a: 123, b: 456"))))
            .containsExactly(new KeyValue(123.0, 456.0));
    }

    @Test
    void testGetKeyValues_multiKey_cannotIndexParallelArrays() throws Exception {
        Index<?> index = new MemoryUniqueIndex("name", Arrays.asList(
            new IndexKey("a", true),
            new IndexKey("b", true)
        ), false);

        assertThatExceptionOfType(CannotIndexParallelArraysError.class)
            .isThrownBy(() -> index.getKeyValues(jsonDocument("a: ['abc'], b: [1, 2, 3]")))
            .withMessage("[Error 171] cannot index parallel arrays [b] [a]");
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/98
    @Test
    void testGetKeyValues_multiKey_document_nested_objects_cannotIndexParallelArrays() throws Exception {
        Index<?> index = new MemoryUniqueIndex("name", Arrays.asList(
            new IndexKey("stock.size", true),
            new IndexKey("stock.quantity", true)
        ), false);

        assertThatExceptionOfType(CannotIndexParallelArraysError.class)
            .isThrownBy(() -> index.getKeyValues(jsonDocument("stock: {size: ['S', 'M'], quantity: [10, 20]}")))
            .withMessage("[Error 171] cannot index parallel arrays [quantity] [size]");
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/98
    @Test
    void testGetKeyValues_multiKey_document_nested_objects() throws Exception {
        Index<?> index = new MemoryUniqueIndex("name", Arrays.asList(
            new IndexKey("stock.size", true),
            new IndexKey("stock.quantity", true)
        ), false);

        assertThat(index.getKeyValues(new Document("stock", Arrays.asList(
            new Document("size", "S").append("color", "red").append("quantity", 25),
            new Document("size", "S").append("color", "blue").append("quantity", 10),
            new Document("size", "M").append("color", "blue").append("quantity", 50)
        )))).containsExactly(
            new KeyValue("S", 25.0),
            new KeyValue("S", 10.0),
            new KeyValue("M", 50.0)
        );
    }

    @Test
    void testGetKeyValues_multiKey_nested_objects_multiple_keys() throws Exception {
        Index<?> index = new MemoryUniqueIndex("name", Arrays.asList(
            new IndexKey("item", true),
            new IndexKey("stock.size", true),
            new IndexKey("stock.quantity", true)
        ), false);

        assertThat(index.getKeyValues(
            new Document("stock", Arrays.asList(
                new Document("size", "S").append("color", "red").append("quantity", 25),
                new Document("size", "S").append("color", "blue").append("quantity", 10),
                new Document("size", "M").append("color", "blue").append("quantity", 50)
            ))
                .append("item", "abc")
        )).containsExactly(
            new KeyValue("abc", "S", 25.0),
            new KeyValue("abc", "S", 10.0),
            new KeyValue("abc", "M", 50.0)
        );
    }

    private static Document jsonDocument(String json) {
        return convert(json(json));
    }

    private static Document convert(org.bson.Document bsonDocument) {
        Document document = new Document();
        for (Entry<String, Object> entry : bsonDocument.entrySet()) {
            document.put(entry.getKey(), convert(entry.getValue()));
        }
        return document;
    }

    private static Object convert(Object value) {
        if (value instanceof org.bson.Document) {
            return convert((org.bson.Document) value);
        } else {
            return value;
        }
    }

}
