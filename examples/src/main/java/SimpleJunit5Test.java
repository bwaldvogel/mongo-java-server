import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetSocketAddress;

import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;

import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;

class SimpleJunit5Test {

    private MongoCollection<Document> collection;
    private MongoClient client;
    private MongoServer server;

    @BeforeEach
    void setUp() {
        server = new MongoServer(new MemoryBackend());

        // bind on a random local port
        InetSocketAddress serverAddress = server.bind();

        client = new MongoClient(new ServerAddress(serverAddress));
        collection = client.getDatabase("testdb").getCollection("testcollection");
    }

    @AfterEach
    void tearDown() {
        client.close();
        server.shutdown();
    }

    @Test
    public void testSimpleInsertQuery() throws Exception {
        assertThat(collection.countDocuments()).isZero();

        // creates the database and collection in memory and insert the object
        Document obj = new Document("_id", 1).append("key", "value");
        collection.insertOne(obj);

        assertThat(collection.countDocuments()).isEqualTo(1L);
        assertThat(collection.find().first()).isEqualTo(obj);
    }

}
