package de.bwaldvogel.mongo.backend.memory;

import static org.fest.assertions.Assertions.assertThat;

import java.io.File;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DBObject;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.backend.AbstractBackendTest;
import de.bwaldvogel.mongo.backend.h2.H2Backend;

public class H2OnDiskBackendTest extends AbstractBackendTest {

    private static final Logger log = LoggerFactory.getLogger(H2OnDiskBackendTest.class);

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private File tempFile;

    @Override
    public void setUp() throws Exception {
        tempFile = tempFolder.newFile(getClass().getSimpleName() + ".mv");
        log.debug("created {} for testing", tempFile);
        super.setUp();
    }

    @Override
    protected MongoBackend createBackend() throws Exception {
        return new H2Backend(tempFile.toString());
    }

    @Test
    public void testShutdownAndRestart() throws Exception {
        collection.insert(json("_id: 1"));
        collection.insert(json("_id: 2"));

        shutdownServer();
        spinUpServer();

        assertThat(collection.find().toArray()).containsOnly(json("_id: 1"), json("_id: 2"));
    }

    @Test
    public void testShutdownAndRestartOpenesIndexes() throws Exception {
        collection.createIndex(json("a: 1"));
        collection.createIndex(json("b: 1"));
        List<DBObject> indexes = getCollection("system.indexes").find().toArray();
        assertThat(indexes).hasSize(3);

        collection.insert(json("_id: 1"));
        collection.insert(json("_id: 2"));

        List<String> databaseNames = client.getDatabaseNames();

        shutdownServer();
        spinUpServer();

        assertThat(client.getDatabaseNames()).isEqualTo(databaseNames);

        List<DBObject> indexesAfterRestart = getCollection("system.indexes").find().toArray();
        assertThat(indexesAfterRestart).isEqualTo(indexes);
    }

}