package de.bwaldvogel.mongo.backend.memory;

import static org.fest.assertions.Assertions.assertThat;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.CommandResult;
import com.mongodb.DBObject;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.backend.AbstractBackendTest;
import de.bwaldvogel.mongo.backend.h2.H2Backend;

public class H2OnDiskBackendTest extends AbstractBackendTest {

    private static final Logger log = LoggerFactory.getLogger(H2OnDiskBackendTest.class);

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private H2Backend backend;

    private File tempFile;

    @Override
    public void setUp() throws Exception {
        tempFile = tempFolder.newFile(getClass().getSimpleName() + ".mv");
        log.debug("created {} for testing", tempFile);
        super.setUp();
    }

    @Override
    protected MongoBackend createBackend() throws Exception {
        backend = new H2Backend(tempFile.toString());
        return backend;
    }

    @Test
    public void testShutdownAndRestart() throws Exception {
        collection.insert(json("_id: 1"));
        collection.insert(json("_id: 2"));

        restart();

        assertThat(collection.find().toArray()).containsOnly(json("_id: 1"), json("_id: 2"));
    }

    @Test
    public void testShutdownAndRestartOpensDatabasesAndCollections() throws Exception {
        List<String> dbs = Arrays.asList("testdb1", "testdb2");
        for (String db : dbs) {
            for (String coll : new String[] { "collection1", "collection2" }) {
                client.getDB(db).getCollection(coll).insert(json(""));
            }
        }
        List<String> dbNamesBefore = client.getDatabaseNames();
        assertThat(dbNamesBefore).isEqualTo(dbs);

        restart();

        List<String> dbNamesAfter = client.getDatabaseNames();
        assertThat(dbNamesAfter).isEqualTo(dbs);
    }

    @Test
    public void testShutdownAndRestartOpensIndexes() throws Exception {
        collection.createIndex(json("a: 1"));
        collection.createIndex(json("b: 1"));
        List<DBObject> indexes = getCollection("system.indexes").find().toArray();
        assertThat(indexes).hasSize(3);

        collection.insert(json("_id: 1"));
        collection.insert(json("_id: 2"));

        List<String> databaseNames = client.getDatabaseNames();

        restart();

        assertThat(client.getDatabaseNames()).isEqualTo(databaseNames);

        List<DBObject> indexesAfterRestart = getCollection("system.indexes").find().toArray();
        assertThat(indexesAfterRestart).isEqualTo(indexes);
    }

    @Test
    public void testShutdownAndRestartKeepsStatistics() throws Exception {
        collection.createIndex(json("a: 1"));
        collection.createIndex(json("b: 1"));

        collection.insert(json("_id: 1"));
        collection.insert(json("_id: 2"));

        backend.commit();

        CommandResult statsBefore = db.getStats();

        restart();

        CommandResult statsAfter = db.getStats();

        assertThat(statsAfter).isEqualTo(statsBefore);
    }

    private void restart() throws Exception {
        shutdownServer();
        spinUpServer();
    }

}