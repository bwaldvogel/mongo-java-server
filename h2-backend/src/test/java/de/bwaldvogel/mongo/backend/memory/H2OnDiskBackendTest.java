package de.bwaldvogel.mongo.backend.memory;

import static de.bwaldvogel.mongo.backend.TestUtils.json;
import static de.bwaldvogel.mongo.backend.TestUtils.toArray;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.backend.AbstractBackendTest;
import de.bwaldvogel.mongo.backend.h2.H2Backend;

public class H2OnDiskBackendTest extends AbstractBackendTest {

    private static final Logger log = LoggerFactory.getLogger(H2OnDiskBackendTest.class);

    private static H2Backend backend;

    @TempDir
    static Path tempFolder;

    private static Path tempFile;

    @Override
    protected void setUpBackend() throws Exception {
        if (tempFile == null) {
            tempFile = tempFolder.resolve(getClass().getSimpleName() + ".mv");
            log.debug("created {} for testing", tempFile);
        }
        super.setUpBackend();
    }

    @Override
    protected MongoBackend createBackend() throws Exception {
        backend = new H2Backend(tempFile.toString());
        backend.getMvStore().setAutoCommitDelay(0);
        return backend;
    }

    @Test
    void testShutdownAndRestart() throws Exception {
        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));

        long versionBeforeUpdate = backend.getMvStore().commit();

        collection.findOneAndUpdate(json("_id: 2"), json("$set: {x: 10}"));

        long versionAfterUpdate = backend.getMvStore().commit();
        assertThat(versionAfterUpdate).isEqualTo(versionBeforeUpdate + 1);

        restart();

        assertThat(collection.find())
            .containsExactly(json("_id: 1"), json("_id: 2, x: 10"));
    }

    @Test
    void testShutdownAndRestartOpensDatabasesAndCollections() throws Exception {
        List<String> dbs = Arrays.asList("local", "testdb1", "testdb2");
        for (String db : dbs) {
            for (String coll : new String[] { "collection1", "collection2" }) {
                syncClient.getDatabase(db).getCollection(coll).insertOne(json(""));
            }
        }
        List<String> dbNamesBefore = toArray(syncClient.listDatabaseNames());
        assertThat(dbNamesBefore).isEqualTo(dbs);

        restart();

        List<String> dbNamesAfter = toArray(syncClient.listDatabaseNames());
        assertThat(dbNamesAfter).isEqualTo(dbs);
    }

    @Test
    void testShutdownAndRestartOpensIndexes() throws Exception {
        collection.createIndex(json("a: 1"));
        collection.createIndex(json("b: 1"));
        List<Document> indexes = toArray(collection.listIndexes());
        assertThat(indexes).hasSize(3);

        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));

        List<String> databaseNames = toArray(syncClient.listDatabaseNames());

        restart();

        assertThat(syncClient.listDatabaseNames()).containsExactlyElementsOf(databaseNames);
        assertThat(collection.countDocuments()).isEqualTo(2);

        assertThat(collection.listIndexes()).containsExactlyElementsOf(indexes);
    }

    @Test
    void testShutdownAndRestartKeepsStatistics() throws Exception {
        collection.createIndex(json("a: 1"));
        collection.createIndex(json("b: 1"));

        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));

        backend.commit();

        Document statsBefore = db.runCommand(json("dbStats: 1, scale: 1"));

        restart();

        Document statsAfter = db.runCommand(json("dbStats: 1, scale: 1"));

        assertThat(statsAfter).isEqualTo(statsBefore);
    }

}
