package de.bwaldvogel.mongo;

import static de.bwaldvogel.mongo.backend.TestUtils.json;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.Date;

import org.bson.Document;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.mongodb.MongoCommandException;

import de.bwaldvogel.mongo.backend.AbstractBackendTest;

public class RealMongoBackendTest extends AbstractBackendTest {

    @RegisterExtension
    static RealMongoContainer realMongoContainer = new RealMongoContainer();

    @Override
    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        dropAllDatabases();
    }

    @Override
    protected void setUpBackend() throws Exception {
        connectionString = realMongoContainer.getConnectionString();
    }

    @Override
    protected void restart() throws Exception {
        tearDown();
        realMongoContainer.restart();
        setUp();
    }

    @Override
    protected MongoBackend createBackend() throws Exception {
        throw new UnsupportedOperationException();
    }

    private void assumeStrictMode() {
        Assumptions.assumeTrue(Boolean.getBoolean(getClass().getSimpleName() + ".strict"));
    }

    @Test
    @Override
    public void testListDatabaseNames() throws Exception {
        assumeStrictMode();
        super.testListDatabaseNames();
    }

    @Test
    @Override
    public void testCurrentOperations() throws Exception {
        assumeStrictMode();
        super.testCurrentOperations();
    }

    @Test
    @Override
    public void testInsertInSystemNamespace() throws Exception {
        assumeStrictMode();
        super.testInsertInSystemNamespace();
    }

    @Test
    @Override
    public void testQuerySystemNamespace() throws Exception {
        assumeStrictMode();
        super.testQuerySystemNamespace();
    }

    @Test
    @Override
    public void testSystemNamespaces() throws Exception {
        assumeStrictMode();
        super.testSystemNamespaces();
    }

    @Test
    @Override
    public void testUpdateInSystemNamespace() throws Exception {
        assumeStrictMode();
        super.testUpdateInSystemNamespace();
    }

    @Test
    @Override
    public void testGetLogStartupWarnings() throws Exception {
        assumeStrictMode();
        super.testGetLogStartupWarnings();
    }

    @Test
    @Override
    public void testReservedCollectionNames() throws Exception {
        assumeStrictMode();
        super.testReservedCollectionNames();
    }

    @Test
    @Override
    public void testQueryWithSubdocumentIndex() throws Exception {
        assumeStrictMode();
        super.testQueryWithSubdocumentIndex();
    }

    @Test
    @Override
    public void testServerStatus() throws Exception {
        verifyServerStatus(runCommand("serverStatus"));
        verifyServerStatus(db.runCommand(json("serverStatus:1")));
    }

    private void verifyServerStatus(Document serverStatus) {
        assertThat(serverStatus.getDouble("ok")).isEqualTo(1);
        assertThat(serverStatus.get("uptime")).isInstanceOf(Number.class);
        assertThat(serverStatus.get("uptimeMillis")).isInstanceOf(Long.class);
        assertThat(serverStatus.get("localTime")).isInstanceOf(Date.class);

        Document connections = (Document) serverStatus.get("connections");
        assertThat(connections.get("current")).isNotNull();
    }

    @Test
    @Disabled
    @Override
    public void testCursor_iteratingACursorThatNoLongerExists() {
        // disabled on real MongoDB
    }

    @Test
    @Override
    public void testResetError() throws Exception {
        // Note: No longer implemented since MongoDB 5: "Command failed with error 59 (CommandNotFound): 'no such command: 'reseterror'"
        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> db.runCommand(json("reseterror: 1")))
            .withMessageStartingWith("Command failed with error 59 (CommandNotFound): 'no such command: 'reseterror'");
    }

    @Test
    @Override
    public void testCommandThatTriggersAnInternalException() throws Exception {
        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> db.runCommand(json("triggerInternalException: 1")))
            .withMessageStartingWith("Command failed with error 59 (CommandNotFound): 'no such command: 'triggerInternalException'");
    }
}
