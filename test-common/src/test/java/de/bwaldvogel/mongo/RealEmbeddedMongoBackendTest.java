package de.bwaldvogel.mongo;

import java.net.InetSocketAddress;

import org.junit.AfterClass;
import org.junit.Assume;

import de.bwaldvogel.mongo.backend.AbstractBackendTest;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;

public class RealEmbeddedMongoBackendTest extends AbstractBackendTest {

    private static final MongodStarter starter = MongodStarter.getDefaultInstance();
    private static MongodExecutable mongodExecutable;

    @Override
    protected void setUpBackend() throws Exception {
        String bindIp = "localhost";
        int port = 12345;
        IMongodConfig mongodConfig = new MongodConfigBuilder()
            .version(Version.Main.PRODUCTION)
            .net(new Net(bindIp, port, Network.localhostIsIPv6()))
            .build();
        serverAddress = new InetSocketAddress(bindIp, port);
        mongodExecutable = starter.prepare(mongodConfig);
        mongodExecutable.start();
    }

    @AfterClass
    public static void tearDownServer() {
        mongodExecutable.stop();
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
    public void testSystemIndexes() throws Exception {
        assumeStrictMode();
        super.testSystemIndexes();
    }

    @Override
    public void testListIndexes() throws Exception {
        assumeStrictMode();
        super.testListIndexes();
    }

    @Override
    public void testQueryWithSubdocumentIndex() throws Exception {
        assumeStrictMode();
        super.testQueryWithSubdocumentIndex();
    }

    @Override
    public void testCreateIndexes() {
        assumeStrictMode();
        super.testCreateIndexes();
    }
}
