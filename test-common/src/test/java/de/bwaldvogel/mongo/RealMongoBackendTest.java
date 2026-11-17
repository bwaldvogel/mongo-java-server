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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.mongodb.MongoCommandException;

import de.bwaldvogel.mongo.backend.AbstractBackendTest;
import de.bwaldvogel.mongo.backend.Command;

public class RealMongoBackendTest extends AbstractBackendTest {

    @RegisterExtension
    static RealMongoContainer realMongoContainer = new RealMongoContainer();

    @Override
    @BeforeEach
    public void setUp() {
        super.setUp();
        dropAllDatabases();
    }

    @Override
    protected void setUpBackend() {
        connectionString = realMongoContainer.getConnectionString();
    }

    @Override
    protected void restart() {
        tearDown();
        realMongoContainer.restart();
        setUp();
    }

    @Override
    protected MongoBackend createBackend() {
        throw new UnsupportedOperationException();
    }

    @Override
    @ParameterizedTest
    @CsvSource({"tailable,true", "awaitData,true", "tailable,false", "awaitData,false"})
    public void testUnsupportedCursorOptions(String option, boolean collectionExists) {
        if (collectionExists) {
            collection.insertOne(json("_id: 1"));
        }
        Document find = new Document("find", collection.getNamespace().getCollectionName()).append(option, true);
        if (option.equals("tailable") && !collectionExists) {
            Document cursor = db.runCommand(find).get("cursor", Document.class);
            assertThat(cursor.getList("firstBatch", Document.class)).isEmpty();
            assertThat(cursor.getLong("id")).isZero();
        } else {
            assertThatExceptionOfType(MongoCommandException.class)
                .isThrownBy(() -> db.runCommand(find))
                .withMessageContaining(option.equals("awaitData") ? "awaitData" : "non capped")
                .extracting(MongoCommandException::getErrorCode).isEqualTo(option.equals("awaitData") ? 9 : 2);
        }
    }

    private void assumeStrictMode() {
        Assumptions.assumeTrue(Boolean.getBoolean(getClass().getSimpleName() + ".strict"));
    }

    @Test
    @Override
    public void testListDatabaseNames() {
        assumeStrictMode();
        super.testListDatabaseNames();
    }

    @Test
    @Override
    public void testCurrentOperations() {
        assumeStrictMode();
        super.testCurrentOperations();
    }

    @Test
    @Override
    public void testInsertInSystemNamespace() {
        assumeStrictMode();
        super.testInsertInSystemNamespace();
    }

    @Test
    @Override
    public void testQuerySystemNamespace() {
        assumeStrictMode();
        super.testQuerySystemNamespace();
    }

    @Test
    @Override
    public void testSystemNamespaces() {
        assumeStrictMode();
        super.testSystemNamespaces();
    }

    @Test
    @Override
    public void testUpdateInSystemNamespace() {
        assumeStrictMode();
        super.testUpdateInSystemNamespace();
    }

    @Test
    @Override
    public void testGetLogStartupWarnings() {
        assumeStrictMode();
        super.testGetLogStartupWarnings();
    }

    @Test
    @Override
    public void testReservedCollectionNames() {
        assumeStrictMode();
        super.testReservedCollectionNames();
    }

    @Test
    @Override
    public void testQueryWithSubdocumentIndex() {
        assumeStrictMode();
        super.testQueryWithSubdocumentIndex();
    }

    @Test
    @Override
    public void testServerStatus() {
        verifyServerStatus(runCommand(Command.SERVER_STATUS));
        verifyServerStatus(db.runCommand(json("serverStatus: 1")));
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
    public void testCommandThatTriggersAnInternalException() {
        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> db.runCommand(json("triggerInternalException: 1")))
            .withMessageStartingWith("Command execution failed on MongoDB server with error 59 (CommandNotFound): 'no such command: 'triggerInternalException'");
    }
}
