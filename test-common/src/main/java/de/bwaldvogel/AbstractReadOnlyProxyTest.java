package de.bwaldvogel;

import static de.bwaldvogel.mongo.backend.TestUtils.json;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.ReadOnlyProxy;

public abstract class AbstractReadOnlyProxyTest {

    private MongoClient readOnlyClient;
    private MongoServer mongoServer;
    private MongoServer writeableServer;
    private MongoClient writeClient;

    protected abstract MongoBackend createBackend() throws Exception;

    @BeforeEach
    public void setUp() throws Exception {
        MongoBackend mongoBackend = createBackend();
        writeableServer = new MongoServer(mongoBackend);
        writeClient = new MongoClient(new ServerAddress(writeableServer.bind()));

        mongoServer = new MongoServer(new ReadOnlyProxy(mongoBackend));
        readOnlyClient = new MongoClient(new ServerAddress(mongoServer.bind()));
    }

    @AfterEach
    public void tearDown() {
        writeClient.close();
        readOnlyClient.close();
        mongoServer.shutdownNow();
        writeableServer.shutdownNow();
    }

    @Test
    void testMaxBsonSize() throws Exception {
        int maxBsonObjectSize = readOnlyClient.getMaxBsonObjectSize();
        assertThat(maxBsonObjectSize).isEqualTo(16777216);
    }

    @Test
    void testServerStatus() throws Exception {
        readOnlyClient.getDatabase("admin").runCommand(new Document("serverStatus", 1));
    }

    @Test
    void testCurrentOperations() throws Exception {
        Document currentOperations = readOnlyClient.getDatabase("admin").getCollection("$cmd.sys.inprog").find().first();
        assertThat(currentOperations).isNotNull();
    }

    @Test
    void testStats() throws Exception {
        Document stats = readOnlyClient.getDatabase("testdb").runCommand(json("dbStats:1"));
        assertThat(((Number) stats.get("objects")).longValue()).isZero();
    }

    @Test
    void testListDatabaseNames() throws Exception {
        assertThat(readOnlyClient.listDatabaseNames()).isEmpty();
        writeClient.getDatabase("testdb").getCollection("testcollection").insertOne(new Document());
        assertThat(readOnlyClient.listDatabaseNames()).containsExactly("testdb");
        writeClient.getDatabase("bar").getCollection("testcollection").insertOne(new Document());
        assertThat(readOnlyClient.listDatabaseNames()).containsExactly("bar", "testdb");
    }

    @Test
    void testIllegalCommand() throws Exception {
        assertThatExceptionOfType(MongoException.class)
            .isThrownBy(() -> readOnlyClient.getDatabase("testdb").runCommand(json("foo:1")))
            .withMessageContaining("Command failed with error 59 (CommandNotFound): 'no such command: 'foo'");

        assertThatExceptionOfType(MongoException.class)
            .isThrownBy(() -> readOnlyClient.getDatabase("bar").runCommand(json("foo:1")))
            .withMessageContaining("Command failed with error 59 (CommandNotFound): 'no such command: 'foo'");
    }

    @Test
    void testQuery() throws Exception {
        MongoCollection<Document> collection = readOnlyClient.getDatabase("testdb").getCollection("testcollection");
        Document obj = collection.find(json("_id: 1")).first();
        assertThat(obj).isNull();
        assertThat(collection.count()).isEqualTo(0);
    }

    @Test
    void testDistinctQuery() {
        MongoCollection<Document> collection = writeClient.getDatabase("testdb").getCollection("testcollection");
        collection.insertOne(new Document("n", 1));
        collection.insertOne(new Document("n", 2));
        collection.insertOne(new Document("n", 1));
        collection = readOnlyClient.getDatabase("testdb").getCollection("testcollection");
        assertThat(collection.distinct("n", Integer.class)).containsExactly(1, 2);
    }

    @Test
    void testInsert() throws Exception {
        MongoCollection<Document> collection = readOnlyClient.getDatabase("testdb").getCollection("testcollection");
        assertThat(collection.count()).isZero();

        assertThatExceptionOfType(MongoException.class)
            .isThrownBy(() -> collection.insertOne(json("{}")));
    }

    @Test
    void testUpdate() throws Exception {
        MongoCollection<Document> collection = readOnlyClient.getDatabase("testdb").getCollection("testcollection");
        Document object = new Document("_id", 1);
        Document newObject = new Document("_id", 1);

        assertThatExceptionOfType(MongoException.class)
            .isThrownBy(() -> collection.replaceOne(object, newObject))
            .withMessageContaining("Command failed with error 59 (CommandNotFound): 'no such command: 'update'");
    }

    @Test
    void testUpsert() throws Exception {
        MongoCollection<Document> collection = readOnlyClient.getDatabase("testdb").getCollection("testcollection");

        assertThatExceptionOfType(MongoException.class)
            .isThrownBy(() -> collection.updateMany(json("{}"), Updates.set("foo", "bar"), new UpdateOptions().upsert(true)))
            .withMessageContaining("Command failed with error 59 (CommandNotFound): 'no such command: 'update'");
    }

    @Test
    void testDropDatabase() throws Exception {
        assertThatExceptionOfType(MongoException.class)
            .isThrownBy(() -> readOnlyClient.dropDatabase("testdb"));
    }

    @Test
    void testDropCollection() throws Exception {
        MongoCollection<Document> collection = readOnlyClient.getDatabase("testdb").getCollection("foo");

        assertThatExceptionOfType(MongoException.class)
            .isThrownBy(collection::drop)
            .withMessageContaining("Command failed with error 59 (CommandNotFound): 'no such command: 'drop'");
    }

}
