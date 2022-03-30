package de.bwaldvogel.mongo.backend.memory;

import com.mongodb.client.model.Filters;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.backend.AbstractBackendTest;

import org.bson.Document;
import org.junit.jupiter.api.Test;

public class MemoryBackendTest extends AbstractBackendTest {

    @Override
    protected MongoBackend createBackend() {
        return new MemoryBackend(clock);
    }

    @Test
    public void testLongIndex() {
        long id1 = 223372036854775806L;
        long id2 = 223372036854775800L;
        // GIVEN there are no items in the collection having the given ids
        assertThat(collection.find(Filters.eq(id1)).first()).isNull();
        assertThat(collection.find(Filters.eq(id2)).first()).isNull();

        // WHEN we insert an item with id #1
        collection.insertOne(new Document("_id", id1).append("name", "item 1"));

        // THEN the collections has the item
        assertThat(collection.find(Filters.eq(id1)).first()).isNotNull();
        // AND the collection DOES NOT have an item with id #2
        assertThat(collection.find(Filters.eq(id2)).first()).isNull();
    }

}
