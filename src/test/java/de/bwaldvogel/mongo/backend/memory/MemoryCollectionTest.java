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
