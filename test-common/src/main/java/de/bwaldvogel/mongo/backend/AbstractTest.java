package de.bwaldvogel.mongo.backend;

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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import com.mongodb.ConnectionString;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.wire.message.MongoKillCursors;

public abstract class AbstractTest {

    protected static final String ADMIN_DB_NAME = "admin";
    protected static final String TEST_DATABASE_NAME = "testdb";

    protected static final TestClock clock = TestClock.defaultClock();

    protected static MongoClient syncClient;
    protected static MongoDatabase db;

    protected static MongoCollection<Document> collection;

    static com.mongodb.reactivestreams.client.MongoCollection<Document> asyncCollection;
    static com.mongodb.reactivestreams.client.MongoDatabase asyncDb;

    private static MongoServer mongoServer;
    private static com.mongodb.reactivestreams.client.MongoClient asyncClient;
    protected static ConnectionString connectionString;
    protected static MongoBackend backend;

    protected abstract MongoBackend createBackend() throws Exception;

    @BeforeEach
    public void setUp() throws Exception {
        clock.reset();
        if (connectionString == null) {
            setUpBackend();
            setUpClients();
        } else {
            dropAllDatabases();
        }
    }

    @AfterEach
    void assertNoOpenCursors() throws Exception {
        Document serverStatus = runCommand("serverStatus");
        assertThat(serverStatus.getDouble("ok")).isEqualTo(1);
        Document openCursors = serverStatus.get("metrics", Document.class)
            .get("cursor", Document.class)
            .get("open", Document.class);

        Long totalOpenCursors = openCursors.getLong("total");
        assertThat(totalOpenCursors).isZero();
    }

    protected void dropAllDatabases() {
        for (String databaseName : syncClient.listDatabaseNames()) {
            if (databaseName.equals("admin") || databaseName.equals("local")) {
                continue;
            }
            syncClient.getDatabase(databaseName).drop();
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

    protected static void setUpClients() throws Exception {
        syncClient = MongoClients.create(connectionString);
        asyncClient = com.mongodb.reactivestreams.client.MongoClients.create(connectionString);
        db = syncClient.getDatabase(TEST_DATABASE_NAME);
        collection = db.getCollection("testcoll");

        MongoNamespace namespace = collection.getNamespace();
        asyncDb = asyncClient.getDatabase(namespace.getDatabaseName());
        asyncCollection = asyncDb.getCollection(namespace.getCollectionName());
    }

    protected void setUpBackend() throws Exception {
        backend = createBackend();
        mongoServer = new MongoServer(backend);
        connectionString = new ConnectionString(mongoServer.bindAndGetConnectionString());
    }

    protected static void closeClients() {
        if (syncClient != null) {
            syncClient.close();
        }
        if (asyncClient != null) {
            asyncClient.close();
        }
    }

    private static void tearDownBackend() {
        if (mongoServer != null) {
            mongoServer.shutdownNow();
            mongoServer = null;
        }
        connectionString = null;
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

    protected Document runCommand(String commandName) {
        return runCommand(new Document(commandName, 1));
    }

    protected Document runCommand(Document command) {
        return getAdminDb().runCommand(command);
    }

    protected MongoDatabase getAdminDb() {
        return syncClient.getDatabase(ADMIN_DB_NAME);
    }
}
