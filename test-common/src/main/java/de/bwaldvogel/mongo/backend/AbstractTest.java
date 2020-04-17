package de.bwaldvogel.mongo.backend;

import static de.bwaldvogel.mongo.backend.TestUtils.json;

import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import org.assertj.core.api.AbstractBooleanAssert;
import org.assertj.core.api.AbstractDoubleAssert;
import org.assertj.core.api.AbstractIntegerAssert;
import org.assertj.core.api.AbstractLongAssert;
import org.assertj.core.api.AbstractStringAssert;
import org.assertj.core.api.AbstractThrowableAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.IterableAssert;
import org.assertj.core.api.MapAssert;
import org.assertj.core.api.ObjectAssert;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;

import com.mongodb.MongoNamespace;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.reactivestreams.client.MongoClient;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.wire.message.MongoKillCursors;

public abstract class AbstractTest {

    protected static final String TEST_DATABASE_NAME = "testdb";
    protected static final String LOCAL_DATABASE = "local";
    protected static final String OPLOG_COLLECTION_NAME = "oplog.rs";

    protected static final Clock TEST_CLOCK = Clock.fixed(Instant.parse("2019-05-23T12:00:00.123Z"), ZoneOffset.UTC);

    protected static com.mongodb.MongoClient syncClient;
    protected static MongoDatabase db;
    protected static MongoDatabase localDb;

    protected static MongoCollection<Document> collection;
    protected static MongoCollection<Document> oplogCollection;

    static com.mongodb.reactivestreams.client.MongoCollection<Document> asyncCollection;

    private static MongoServer mongoServer;
    private static MongoClient asyncClient;
    protected static InetSocketAddress serverAddress;

    protected abstract MongoBackend createBackend() throws Exception;

    @BeforeEach
    public void setUp() throws Exception {
        if (serverAddress == null) {
            setUpBackend();
            setUpClients();
        } else {
            dropAllDatabases();
            clearOplog();
        }
    }

    protected void dropAllDatabases() {
        for (String databaseName : syncClient.listDatabaseNames()) {
            if (databaseName.equals("admin") || databaseName.equals("local")) {
                continue;
            }
            syncClient.dropDatabase(databaseName);
        }
    }

    protected void clearOplog() {
        if (oplogCollection != null) {
            oplogCollection.deleteMany(json(""));
        }
    }

    protected void killCursors(List<Long> cursorIds) {
        MongoKillCursors killCursors = new MongoKillCursors(cursorIds);
        mongoServer.closeCursors(killCursors);
    }

    @AfterAll
    public static void tearDown() {
        closeClients();
        tearDownBackend();
    }

    private static void setUpClients() throws Exception {
        syncClient = new com.mongodb.MongoClient(new ServerAddress(serverAddress));
        asyncClient = com.mongodb.reactivestreams.client.MongoClients.create("mongodb://" + serverAddress.getHostName() + ":" + serverAddress.getPort());
        db = syncClient.getDatabase(TEST_DATABASE_NAME);
        localDb = syncClient.getDatabase(LOCAL_DATABASE);
        collection = db.getCollection("testcoll");
        oplogCollection = localDb.getCollection(OPLOG_COLLECTION_NAME);

        MongoNamespace namespace = collection.getNamespace();
        com.mongodb.reactivestreams.client.MongoDatabase asyncDb = asyncClient.getDatabase(namespace.getDatabaseName());
        asyncCollection = asyncDb.getCollection(namespace.getCollectionName());
    }

    protected void setUpBackend() throws Exception {
        MongoBackend backend = createBackend();
        backend.setClock(TEST_CLOCK);
        mongoServer = new MongoServer(backend, true);
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

    protected static MapAssert<String, Object> assertThat(Document actual) {
        return Assertions.assertThat(actual);
    }

    protected static AbstractLongAssert<?> assertThat(Long actual) {
        return Assertions.assertThat(actual);
    }

    protected static AbstractDoubleAssert<?> assertThat(Double actual) {
        return Assertions.assertThat(actual);
    }

    protected static AbstractStringAssert<?> assertThat(String actual) {
        return Assertions.assertThat(actual);
    }

    protected static <T> ObjectAssert<T> assertThat(T actual) {
        return Assertions.assertThat(actual);
    }

    protected static AbstractIntegerAssert<?> assertThat(Integer actual) {
        return Assertions.assertThat(actual);
    }

    protected static AbstractBooleanAssert<?> assertThat(Boolean actual) {
        return Assertions.assertThat(actual);
    }

    protected static <T> IterableAssert<T> assertThat(Iterable<T> actual) {
        // improve assertion array by collection entire array
        List<T> values = TestUtils.toArray(actual);
        return Assertions.assertThat((Iterable<? extends T>) values);
    }

    protected static AbstractThrowableAssert<?, ? extends Throwable> assertThat(Throwable actual) {
        return Assertions.assertThat(actual);
    }

}
