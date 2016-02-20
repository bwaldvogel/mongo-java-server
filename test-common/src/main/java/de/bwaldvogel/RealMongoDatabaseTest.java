package de.bwaldvogel;

import static de.bwaldvogel.mongo.backend.TestUtils.json;
import static de.bwaldvogel.mongo.backend.TestUtils.toArray;
import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.util.List;

import org.bson.Document;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.DBRef;
import com.mongodb.MongoClient;
import com.mongodb.MongoQueryException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;

public class RealMongoDatabaseTest {

    private MongoClient mongoClient;
    private MongoCollection<Document> collection;

    @Before
    public void setUp() throws Exception {
        Assume.assumeTrue(Boolean.getBoolean(getClass().getName() + ".enabled"));
        mongoClient = new MongoClient("localhost");
        collection = mongoClient.getDatabase("testdb").getCollection(getClass().getSimpleName());
        collection.deleteMany(new Document());
    }

    @After
    public void tearDown() throws Exception {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    @Test
    public void testEmptyArrayQuery() throws Exception {
        collection.insertOne(json("_id: 1"));

        try {
            collection.find(Filters.and()).first();
            fail("MongoQueryException expected");
        } catch (MongoQueryException e) {
            assertThat(e.getMessage()).contains("must be a nonempty array");
        }
    }

    @Test
    public void testFindAllReferences() throws Exception {
        collection.insertOne(new Document("_id", 1).append("ref", new DBRef("coll1", 1)));
        collection.insertOne(new Document("_id", 2).append("ref", new DBRef("coll1", 2)));
        collection.insertOne(new Document("_id", 3).append("ref", new DBRef("coll2", 1)));
        collection.insertOne(new Document("_id", 4).append("ref", new DBRef("coll2", 2)));

        List<Document> documents = toArray(collection.find(json("ref: {$ref: 'coll1', $id: 1}")).projection(json("_id: 1")));
        assertThat(documents).containsExactly(json("_id: 1"));
    }
}
