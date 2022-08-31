import static org.assertj.core.api.Assertions.assertThat;

import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;

import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;

class SimpleJUnit5WithModernClientTest {

    private MongoCollection<Document> collection;
    private MongoClient client;
    private MongoServer server;

    @BeforeEach
    void setUp() {
        server = new MongoServer(new MemoryBackend());

        // bind on a random local port
        String connectionString = server.bindAndGetConnectionString();

        client = MongoClients.create(connectionString);
        collection = client.getDatabase("testdb").getCollection("testcollection");
    }

    @AfterEach
    void tearDown() {
        client.close();
        server.shutdown();
    }

    @Test
    void testSimpleInsertQuery() throws Exception {
        assertThat(collection.countDocuments()).isZero();

        // creates the database and collection in memory and insert the object
        Document obj = new Document("_id", 1).append("key", "value");
        collection.insertOne(obj);

        assertThat(collection.countDocuments()).isEqualTo(1L);
        assertThat(collection.find().first()).isEqualTo(obj);
    }

}
