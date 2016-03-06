package de.bwaldvogel.mongo.backend;

import static de.bwaldvogel.mongo.backend.TestUtils.json;

import java.net.InetSocketAddress;

import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.MongoNamespace;
import com.mongodb.ServerAddress;
import com.mongodb.async.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.MongoServer;

public abstract class AbstractSimpleBackendTest {

    private MongoServer mongoServer;

    protected com.mongodb.MongoClient syncClient;
    protected com.mongodb.async.client.MongoClient asyncClient;

    protected MongoDatabase db;
    protected MongoCollection<Document> collection;

    protected com.mongodb.async.client.MongoCollection<Document> asyncCollection;
    private com.mongodb.async.client.MongoDatabase asyncDb;

    protected Document runCommand(String commandName) {
        return runCommand(new Document(commandName, Integer.valueOf(1)));
    }

    protected Document runCommand(Document command) {
        return getAdminDb().runCommand(command);
    }

    protected MongoCollection<Document> getCollection(String collectionName) {
        return db.getCollection(collectionName);
    }

    protected MongoDatabase getAdminDb() {
        return syncClient.getDatabase("admin");
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
        syncClient = new com.mongodb.MongoClient(new ServerAddress(serverAddress));
        asyncClient = MongoClients.create("mongodb://" + serverAddress.getHostName() + ":" + serverAddress.getPort());
        db = syncClient.getDatabase("testdb");
        collection = db.getCollection("testcoll");

        MongoNamespace namespace = collection.getNamespace();
        asyncDb = asyncClient.getDatabase(namespace.getDatabaseName());
        asyncCollection = asyncDb.getCollection(namespace.getCollectionName());
    }

    protected void shutdownServer() {
        syncClient.close();
        asyncClient.close();
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
