package de.bwaldvogel.mongo.backend.memory;

import static org.fest.assertions.Assertions.assertThat;

import java.util.Arrays;

import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.BasicDBObject;

public class MemoryCollectionTest {

    private MemoryCollection collection;

    @Before
    public void setUp() {
        collection = new MemoryCollection("db", "coll", "_id");
    }

    @Test
    public void testConvertSelector() throws Exception {
        BasicBSONObject selector = new BasicBSONObject();

        assertThat(collection.convertSelectorToDocument(selector)) //
                .isEqualTo(new BasicBSONObject());

        selector = new BasicBSONObject("_id", 1);
        assertThat(collection.convertSelectorToDocument(selector)) //
                .isEqualTo(new BasicBSONObject("_id", 1));

        selector = new BasicBSONObject("_id", 1).append("$set", new BasicBSONObject("foo", "bar"));
        assertThat(collection.convertSelectorToDocument(selector)) //
                .isEqualTo(new BasicBSONObject("_id", 1));

        selector = new BasicBSONObject("_id", 1).append("e.i", 14);
        assertThat(collection.convertSelectorToDocument(selector)) //
                .isEqualTo(new BasicBSONObject("_id", 1).append("e", new BasicBSONObject("i", 14)));

        selector = new BasicBSONObject("_id", 1).append("e.i.y", new BasicBSONObject("foo", "bar"));
        assertThat(collection.convertSelectorToDocument(selector)) //
                .isEqualTo(new BasicBSONObject("_id", 1).append("e", //
                        new BasicBSONObject("i", new BasicBSONObject("y", //
                                new BasicBSONObject("foo", "bar")))));
    }

    @Test
    public void testDeriveDocumentId() throws Exception {
        assertThat(collection.deriveDocumentId(new BasicBSONObject())).isInstanceOf(ObjectId.class);

        assertThat(collection.deriveDocumentId(new BasicBSONObject("a", 1))) //
                .isInstanceOf(ObjectId.class);

        assertThat(collection.deriveDocumentId(new BasicBSONObject("_id", 1))) //
                .isEqualTo(1);

        assertThat(collection.deriveDocumentId(new BasicBSONObject("_id", new BasicDBObject("$in", Arrays.asList(1))))) //
                .isEqualTo(1);

        assertThat(collection.deriveDocumentId(new BasicBSONObject("_id", new BasicDBObject("$in", Arrays.asList())))) //
                .isInstanceOf(ObjectId.class);
    }

}
