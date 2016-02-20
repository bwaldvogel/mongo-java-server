package de.bwaldvogel;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.fail;

import org.bson.Document;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

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
        collection.insertOne(new Document("_id", 1));

        try {
            collection.find(Filters.and()).first();
            fail("MongoQueryException expected");
        } catch (MongoQueryException e) {
            assertThat(e.getMessage()).contains("must be a nonempty array");
        }
    }
}
