package de.bwaldvogel.mongo.backend;

import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import org.bson.Document;
import org.junit.AfterClass;
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

    protected static final Clock TEST_CLOCK = Clock.fixed(Instant.parse("2019-05-23T12:00:00.123Z"), ZoneOffset.UTC);

    protected static com.mongodb.MongoClient syncClient;
    protected static MongoDatabase db;
    protected static MongoCollection<Document> collection;
    static com.mongodb.reactivestreams.client.MongoCollection<Document> asyncCollection;
    private static MongoServer mongoServer;
    private static MongoClient asyncClient;
    protected static InetSocketAddress serverAddress;

    protected abstract MongoBackend createBackend() throws Exception;

    @Before
    public void setUp() throws Exception {
        if (serverAddress == null) {
            setUpBackend();
            setUpClients();
        } else {
            dropAllDatabases();
        }
    }

    private void dropAllDatabases() {
        for (String databaseName : syncClient.listDatabaseNames()) {
            if (databaseName.equals("admin") || databaseName.equals("local")) {
                continue;
            }
            syncClient.dropDatabase(databaseName);
        }
    }

    @AfterClass
    public static void tearDown() {
        closeClients();
        tearDownBackend();
    }

    private static void setUpClients() throws Exception {
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
        backend.setClock(TEST_CLOCK);
        mongoServer = new MongoServer(backend);
        serverAddress = mongoServer.bind();
    }

    private static void closeClients() {
        syncClient.close();
        asyncClient.close();
    }

    private static void tearDownBackend() {
        if (mongoServer != null) {
            mongoServer.shutdownNow();
            mongoServer = null;
        }
        serverAddress = null;
    }

    protected void restart() throws Exception {
        tearDown();
        setUp();
    }

}
