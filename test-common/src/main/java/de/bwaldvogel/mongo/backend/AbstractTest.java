package de.bwaldvogel.mongo.backend;

import java.net.InetSocketAddress;
import java.util.ArrayList;
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
import org.bson.Document;
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

    protected static final TestClock clock = TestClock.defaultClock();

    protected static com.mongodb.MongoClient syncClient;
    protected static MongoDatabase db;

    protected static MongoCollection<Document> collection;

    static com.mongodb.reactivestreams.client.MongoCollection<Document> asyncCollection;

    private static MongoServer mongoServer;
    protected static MongoClient asyncClient;
    protected static InetSocketAddress serverAddress;
    protected static MongoBackend backend;

    protected abstract MongoBackend createBackend() throws Exception;

    @BeforeEach
    public void setUp() throws Exception {
        clock.reset();
        if (serverAddress == null) {
            setUpBackend();
            setUpClients();
        } else {
            dropAllDatabases();
        }
    }

    protected void dropAllDatabases() {
        for (String databaseName : syncClient.listDatabaseNames()) {
            if (databaseName.equals("admin") || databaseName.equals("local") || databaseName.equals("config")) {
                continue;
            }
            syncClient.dropDatabase(databaseName);
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
        collection = db.getCollection("testcoll");

        MongoNamespace namespace = collection.getNamespace();
        com.mongodb.reactivestreams.client.MongoDatabase asyncDb = asyncClient.getDatabase(namespace.getDatabaseName());
        asyncCollection = asyncDb.getCollection(namespace.getCollectionName());
    }

    protected void setUpBackend() throws Exception {
        backend = createBackend();
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

    protected List<String> listDatabaseNames() {
        List<String> databaseNames = new ArrayList<>();
        for (String databaseName : syncClient.listDatabaseNames()) {
            databaseNames.add(databaseName);
        }
        return databaseNames;
    }

}
