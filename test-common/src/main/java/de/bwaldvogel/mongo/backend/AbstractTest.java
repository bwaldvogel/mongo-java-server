package de.bwaldvogel.mongo.backend;

import java.net.InetSocketAddress;

import org.bson.Document;
import org.junit.After;
import org.junit.Before;

import com.mongodb.MongoNamespace;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.reactivestreams.client.MongoClient;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.MongoServer;

public abstract class AbstractTest {

    protected static final String TEST_DATABASE_NAME = "testdb";

    protected com.mongodb.MongoClient syncClient;
    protected MongoDatabase db;
    protected MongoCollection<Document> collection;
    com.mongodb.reactivestreams.client.MongoCollection<Document> asyncCollection;
    private MongoServer mongoServer;
    private MongoClient asyncClient;
    protected InetSocketAddress serverAddress;

    protected abstract MongoBackend createBackend() throws Exception;

    @Before
    public void setUp() throws Exception {
        setUpBackend();
        setUpClients();
    }

    @After
    public void tearDown() {
        closeClients();
        tearDownBackend();
    }

    private void setUpClients() throws Exception {
        syncClient = new com.mongodb.MongoClient(new ServerAddress(serverAddress));
        asyncClient = com.mongodb.reactivestreams.client.MongoClients.create("mongodb://" + serverAddress.getHostName() + ":" + serverAddress.getPort());
        db = syncClient.getDatabase(TEST_DATABASE_NAME);
        collection = db.getCollection("testcoll");

        MongoNamespace namespace = collection.getNamespace();
        com.mongodb.reactivestreams.client.MongoDatabase asyncDb = asyncClient.getDatabase(namespace.getDatabaseName());
        asyncCollection = asyncDb.getCollection(namespace.getCollectionName());
    }

    protected void setUpBackend() throws Exception {
        MongoBackend backend = createBackend();
        mongoServer = new MongoServer(backend);
        serverAddress = mongoServer.bind();
    }

    private void closeClients() {
        syncClient.close();
        asyncClient.close();
    }

    protected void tearDownBackend() {
        mongoServer.shutdownNow();
    }
}
