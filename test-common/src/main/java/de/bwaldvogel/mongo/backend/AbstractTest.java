package de.bwaldvogel.mongo.backend;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.assertj.core.api.AbstractBooleanAssert;
import org.assertj.core.api.AbstractDateAssert;
import org.assertj.core.api.AbstractDoubleAssert;
import org.assertj.core.api.AbstractInstantAssert;
import org.assertj.core.api.AbstractIntegerAssert;
import org.assertj.core.api.AbstractLongAssert;
import org.assertj.core.api.AbstractStringAssert;
import org.assertj.core.api.AbstractThrowableAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.IterableAssert;
import org.assertj.core.api.MapAssert;
import org.assertj.core.api.ObjectAssert;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.MongoServer;

public abstract class AbstractTest {

    private static final Logger log = LoggerFactory.getLogger(AbstractTest.class);

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
        for (int i = 0; i < 50; i++) {
            long numberOfOpenCursors = getNumberOfOpenCursors();
            if (numberOfOpenCursors == 0) {
                return;
            }
            log.warn("Found {} open cursors. Waiting trial {}", numberOfOpenCursors, i + 1);
            Thread.sleep(100);
        }
        assertThat(getNumberOfOpenCursors()).isZero();
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
        mongoServer.closeCursors(cursorIds);
    }

    @AfterAll
    public static void tearDown() {
        closeClients();
        tearDownBackend();
    }

    private static void setUpClients() throws Exception {
        MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
            .applyConnectionString(connectionString)
            .uuidRepresentation(UuidRepresentation.STANDARD)
            .build();
        syncClient = MongoClients.create(mongoClientSettings);
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

    private static void closeClients() {
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

    protected static AbstractInstantAssert<?> assertThat(Instant actual) {
        return Assertions.assertThat(actual);
    }

    protected static AbstractDateAssert<?> assertThat(Date actual) {
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

    protected Document runCommand(Command commandName) {
        return runCommand(new Document(commandName.getName(), 1));
    }

    protected Document runCommand(Document command) {
        return getAdminDb().runCommand(command);
    }

    protected MongoDatabase getAdminDb() {
        return syncClient.getDatabase(ADMIN_DB_NAME);
    }

    protected long getNumberOfOpenCursors() {
        Document serverStatus = runCommand(Command.SERVER_STATUS);
        assertThat(serverStatus.getDouble("ok")).isEqualTo(1);
        Document metrics = serverStatus.get("metrics", Document.class);
        Document cursorMetrics = metrics.get("cursor", Document.class);
        Document openCursors = cursorMetrics.get("open", Document.class);
        return openCursors.getLong("total");
    }

    protected void awaitNumberOfOpenCursors(long expectedNumberOfOpenCursors) throws Exception {
        long numberOfOpenCursors = 0;
        for (int i = 0; i < 10; i++) {
            numberOfOpenCursors = getNumberOfOpenCursors();
            if (numberOfOpenCursors == expectedNumberOfOpenCursors) {
                return;
            }
            Thread.sleep(10);
        }
        throw new RuntimeException("Failed waiting for " + expectedNumberOfOpenCursors + " open cursors." +
            " Current: " + numberOfOpenCursors + " open cursors");
    }
}
