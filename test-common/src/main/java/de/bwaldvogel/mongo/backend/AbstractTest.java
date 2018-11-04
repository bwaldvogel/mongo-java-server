package de.bwaldvogel.mongo.backend;

import java.net.InetSocketAddress;

import org.bson.Document;
import org.junit.After;
import org.junit.Before;

import com.mongodb.MongoNamespace;
import com.mongodb.ServerAddress;
import com.mongodb.async.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.MongoServer;

public abstract class AbstractTest {

    protected static final String TEST_DATABASE_NAME = "testdb";

    protected com.mongodb.MongoClient syncClient;
    protected MongoDatabase db;
    protected MongoCollection<Document> collection;
    com.mongodb.async.client.MongoCollection<Document> asyncCollection;
    private MongoServer mongoServer;
    private com.mongodb.async.client.MongoClient asyncClient;
    InetSocketAddress serverAddress;

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
        serverAddress = mongoServer.bind();
        syncClient = new com.mongodb.MongoClient(new ServerAddress(serverAddress));
        asyncClient = MongoClients.create("mongodb://" + serverAddress.getHostName() + ":" + serverAddress.getPort());
        db = syncClient.getDatabase(TEST_DATABASE_NAME);
        collection = db.getCollection("testcoll");

        MongoNamespace namespace = collection.getNamespace();
        com.mongodb.async.client.MongoDatabase asyncDb = asyncClient.getDatabase(namespace.getDatabaseName());
        asyncCollection = asyncDb.getCollection(namespace.getCollectionName());
    }

    protected void shutdownServer() {
        syncClient.close();
        asyncClient.close();
        mongoServer.shutdownNow();
    }
}
