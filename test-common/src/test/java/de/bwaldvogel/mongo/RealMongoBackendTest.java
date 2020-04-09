package de.bwaldvogel.mongo;

import java.net.InetSocketAddress;
import java.util.Date;

import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

import de.bwaldvogel.mongo.backend.AbstractBackendTest;

public class RealMongoBackendTest extends AbstractBackendTest {

    private static final Logger log = LoggerFactory.getLogger(RealMongoBackendTest.class);

    private static GenericContainer<?> mongoContainer;

    @BeforeAll
    public static void setUpMongoContainer() {
        if (Boolean.getBoolean("mongo-java-server-use-existing-container")) {
            log.info("Not starting a testcontainer in favor of an existing container.");
            return;
        }
        mongoContainer = RealMongoContainer.start();
    }

    @Override
    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        dropAllDatabases();
    }

    @Override
    protected void setUpBackend() throws Exception {
        if (mongoContainer != null) {
            serverAddress = new InetSocketAddress(mongoContainer.getFirstMappedPort());
        } else {
            serverAddress = new InetSocketAddress("127.0.0.1", 27018);
        }
    }

    @AfterAll
    public static void tearDownServer() {
        if (mongoContainer != null) {
            mongoContainer.stop();
            mongoContainer = null;
        }
    }

    @Override
    protected void restart() throws Exception {
        tearDown();
        tearDownServer();
        setUpMongoContainer();
        setUp();
    }

    @Override
    protected MongoBackend createBackend() throws Exception {
        throw new UnsupportedOperationException();
    }

    private void assumeStrictMode() {
        Assumptions.assumeTrue(Boolean.getBoolean(getClass().getSimpleName() + ".strict"));
    }

    @Override
    public void testListDatabaseNames() throws Exception {
        assumeStrictMode();
        super.testListDatabaseNames();
    }

    @Override
    public void testCurrentOperations() throws Exception {
        assumeStrictMode();
        super.testCurrentOperations();
    }

    @Override
    public void testFieldSelection_selectAndDeselectFields() {
        assumeStrictMode();
        super.testFieldSelection_selectAndDeselectFields();
    }

    @Override
    public void testInsertInSystemNamespace() throws Exception {
        assumeStrictMode();
        super.testInsertInSystemNamespace();
    }

    @Override
    public void testQuerySystemNamespace() throws Exception {
        assumeStrictMode();
        super.testQuerySystemNamespace();
    }

    @Override
    public void testSystemNamespaces() throws Exception {
        assumeStrictMode();
        super.testSystemNamespaces();
    }

    @Override
    public void testUpdateInSystemNamespace() throws Exception {
        assumeStrictMode();
        super.testUpdateInSystemNamespace();
    }

    @Override
    public void testGetLogStartupWarnings() throws Exception {
        assumeStrictMode();
        super.testGetLogStartupWarnings();
    }

    @Override
    public void testReservedCollectionNames() throws Exception {
        assumeStrictMode();
        super.testReservedCollectionNames();
    }

    @Override
    public void testFindOneAndUpdateUpsertReturnBefore() {
        assumeStrictMode();
        super.testFindOneAndUpdateUpsertReturnBefore();
    }

    @Override
    public void testQueryWithSubdocumentIndex() throws Exception {
        assumeStrictMode();
        super.testQueryWithSubdocumentIndex();
    }

    @Override
    public void testServerStatus() throws Exception {
        Document serverStatus = runCommand("serverStatus");
        assertThat(serverStatus.getDouble("ok")).isEqualTo(1);
        assertThat(serverStatus.get("uptime")).isInstanceOf(Number.class);
        assertThat(serverStatus.get("uptimeMillis")).isInstanceOf(Long.class);
        assertThat(serverStatus.get("localTime")).isInstanceOf(Date.class);

        Document connections = (Document) serverStatus.get("connections");
        assertThat(connections.get("current")).isNotNull();
    }

    @Override
    protected String getExpectedPathPrefix_testUpdateAddToSetEach_unknownModifier() {
        // this is probably a bug
        return "value.value";
    }

    @Override
    @Disabled
    public void testCursor_iteratingACursorThatNoLongerExists() {
        // disabled on real MongoDB
    }

    @Override
    @Disabled
    public void testSimpleOplogInsert() {
        // disabled on real MongoDB
    }
}
