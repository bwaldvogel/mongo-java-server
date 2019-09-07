package de.bwaldvogel.mongo;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Date;

import org.bson.Document;
import org.junit.AfterClass;
import org.junit.Assume;

import de.bwaldvogel.mongo.backend.AbstractBackendTest;

public class RealEmbeddedMongoBackendTest extends AbstractBackendTest {

    private static final RealEmbeddedMongo realEmbeddedMongo = new RealEmbeddedMongo();

    @Override
    protected void setUpBackend() throws Exception {
        serverAddress = realEmbeddedMongo.setUp();
    }

    @AfterClass
    public static void tearDownServer() {
        realEmbeddedMongo.stop();
    }

    @Override
    protected void restart() throws Exception {
        tearDown();
        tearDownServer();
        setUp();
    }

    @Override
    protected MongoBackend createBackend() throws Exception {
        throw new UnsupportedOperationException();
    }

    private void assumeStrictMode() {
        Assume.assumeTrue(Boolean.getBoolean(getClass().getSimpleName() + ".strict"));
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
