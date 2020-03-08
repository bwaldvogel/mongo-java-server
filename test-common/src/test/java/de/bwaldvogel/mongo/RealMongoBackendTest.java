package de.bwaldvogel.mongo;

import java.net.InetSocketAddress;
import java.util.Date;

import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;

import de.bwaldvogel.mongo.backend.AbstractBackendTest;

public class RealMongoBackendTest extends AbstractBackendTest {

    private static GenericContainer<?> mongoContainer;

    @BeforeAll
    public static void setUpMongoContainer() {
        mongoContainer = RealMongoContainer.start();
    }

    @Override
    protected void setUpBackend() throws Exception {
        serverAddress = new InetSocketAddress(mongoContainer.getFirstMappedPort());
    }

    @AfterAll
    public static void tearDownServer() {
        mongoContainer.stop();
        mongoContainer = null;
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
    public void testUpdatePushAll() throws Exception {
        assumeStrictMode();
        super.testUpdatePushAll();
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
    public void testUpdateIllegalFieldName() throws Exception {
        assumeStrictMode();
        super.testUpdateIllegalFieldName();
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
}
