package de.bwaldvogel.mongo.backend;

import static de.bwaldvogel.mongo.backend.TestUtils.json;

import java.net.InetSocketAddress;

import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.MongoServer;

public abstract class AbstractSimpleBackendTest {

    private MongoServer mongoServer;

    protected MongoClient client;
    protected MongoDatabase db;
    protected MongoCollection<Document> collection;

    protected Document command(String command) {
        return getAdminDb().runCommand(new Document(command, Integer.valueOf(1)));
    }

    protected MongoCollection<Document> getCollection(String collectionName) {
        return db.getCollection(collectionName);
    }

    protected MongoDatabase getAdminDb() {
        return client.getDatabase("admin");
    }

    protected abstract MongoBackend createBackend() throws Exception;

    @Before
    public void setUp() throws Exception {
        spinUpServer();
    }

    @After
    public void tearDown() {
        shutdownServer();
    }

    protected void spinUpServer() throws Exception {
        MongoBackend backend = createBackend();
        mongoServer = new MongoServer(backend);
        InetSocketAddress serverAddress = mongoServer.bind();
        client = new MongoClient(new ServerAddress(serverAddress));
        db = client.getDatabase("testdb");
        collection = db.getCollection("testcoll");
    }

    protected void shutdownServer() {
        client.close();
        mongoServer.shutdownNow();
    }

    @Test
    public void testSimpleInsert() throws Exception {
        collection.insertOne(json("_id: 1"));
    }

    @Test
    public void testSimpleInsertDelete() throws Exception {
        collection.insertOne(json("_id: 1"));
        collection.deleteOne(json("_id: 1"));
    }

}
