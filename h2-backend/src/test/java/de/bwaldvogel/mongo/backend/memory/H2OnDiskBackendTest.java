package de.bwaldvogel.mongo.backend.memory;

import static de.bwaldvogel.mongo.backend.TestUtils.json;
import static de.bwaldvogel.mongo.backend.TestUtils.toArray;

import java.nio.file.Path;
import java.util.List;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoNamespace;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.IndexOptions;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.backend.AbstractBackendTest;
import de.bwaldvogel.mongo.backend.h2.H2Backend;

class H2OnDiskBackendTest extends AbstractBackendTest {

    private static final Logger log = LoggerFactory.getLogger(H2OnDiskBackendTest.class);

    private static H2Backend backend;

    @TempDir
    static Path tempFolder;

    private static Path tempFile;

    @Override
    protected void setUpBackend() {
        if (tempFile == null) {
            tempFile = tempFolder.resolve(getClass().getSimpleName() + ".mv");
            log.debug("created {} for testing", tempFile);
        }
        super.setUpBackend();
    }

    @Override
    protected MongoBackend createBackend() {
        backend = new H2Backend(tempFile.toString(), clock);
        backend.getMvStore().setAutoCommitDelay(0);
        return backend;
    }

    @Test
    void testRenamedCollectionSurvivesRestart() {
        String oldCollectionName = collection.getNamespace().getCollectionName();
        String newCollectionName = "renamed-collection";
        List<Document> documents = List.of(json("_id: 1, value: 'first'"), json("_id: 2, value: 'second'"));
        collection.insertMany(documents);
        Document statsBefore = db.runCommand(new Document("collStats", oldCollectionName));

        collection.renameCollection(new MongoNamespace(db.getName(), newCollectionName));
        restart();

        assertThat(db.listCollectionNames()).contains(newCollectionName).doesNotContain(oldCollectionName);
        var renamedCollection = db.getCollection(newCollectionName);
        assertThat(renamedCollection.find().sort(json("_id: 1"))).containsExactlyElementsOf(documents);
        Document statsAfter = db.runCommand(new Document("collStats", newCollectionName));
        assertThat(statsAfter.getInteger("size")).isEqualTo(statsBefore.getInteger("size"));

        renamedCollection.insertOne(json("_id: 3"));
        assertThat(renamedCollection.find().sort(json("_id: 1")))
            .containsExactly(documents.get(0), documents.get(1), json("_id: 3"));
    }

    @Test
    void testCappedCollectionShutdownAndRestart() {
        db.createCollection("capped", new CreateCollectionOptions().capped(true).sizeInBytes(512).maxDocuments(3));
        var capped = db.getCollection("capped");
        capped.insertMany(List.of(json("_id: 30"), json("_id: 10"), json("_id: 20")));
        capped.deleteOne(json("_id: 10"));
        capped.insertOne(json("_id: 40"));
        restart();
        capped = db.getCollection("capped");
        assertThat(capped.find()).containsExactly(json("_id: 30"), json("_id: 20"), json("_id: 40"));
        capped.insertOne(json("_id: 50"));
        assertThat(capped.find()).containsExactly(json("_id: 20"), json("_id: 40"), json("_id: 50"));
        capped.renameCollection(new MongoNamespace(db.getName(), "renamed-capped"));
        restart();
        capped = db.getCollection("renamed-capped");
        capped.insertOne(json("_id: 60"));
        assertThat(capped.find()).containsExactly(json("_id: 40"), json("_id: 50"), json("_id: 60"));
        capped.deleteMany(json("{}"));
        restart();
        capped = db.getCollection("renamed-capped");
        capped.insertMany(List.of(json("_id: 90"), json("_id: 70"), json("_id: 80"), json("_id: 100")));
        assertThat(capped.find()).containsExactly(json("_id: 70"), json("_id: 80"), json("_id: 100"));
        capped.insertOne(new Document("_id", 110).append("data", "x".repeat(490)));
        assertThat(capped.find().projection(json("_id: 1"))).containsExactly(json("_id: 110"));
    }

    @Test
    void testShutdownAndRestart() {
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
    void testShutdownAndRestartOpensDatabasesAndCollections() {
        List<String> dbs = List.of("local", "testdb1", "testdb2");
        for (String db : dbs) {
            for (String coll : new String[] { "collection1", "collection2" }) {
                syncClient.getDatabase(db).getCollection(coll).insertOne(json(""));
            }
        }
        assertThat(syncClient.listDatabaseNames())
            .isEqualTo(dbs);

        restart();

        assertThat(syncClient.listDatabaseNames())
            .isEqualTo(dbs);
    }

    @Test
    void testShutdownAndRestartOpensIndexes() {
        collection.createIndex(json("a: 1"));
        collection.createIndex(json("b: 1"));
        assertThat(collection.listIndexes()).hasSize(3);

        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));

        List<String> databaseNames = toArray(syncClient.listDatabaseNames());

        restart();

        assertThat(syncClient.listDatabaseNames()).containsExactlyElementsOf(databaseNames);
        assertThat(collection.countDocuments()).isEqualTo(2);

        assertThat(collection.listIndexes()).containsExactlyElementsOf(toArray(collection.listIndexes()));
    }

    @Test
    void testShutdownAndRestartKeepsStatistics() {
        collection.createIndex(json("a: 1"), new IndexOptions().unique(true));
        collection.createIndex(json("b: 1"), new IndexOptions().unique(true));

        collection.insertOne(json("_id: 1, a: 10, b: 100"));
        collection.insertOne(json("_id: 2, a: 20, b: 200"));

        backend.commit();

        Document statsBefore = db.runCommand(json("dbStats: 1, scale: 1"));

        restart();

        Document statsAfter = db.runCommand(json("dbStats: 1, scale: 1"));

        assertThat(statsAfter).isEqualTo(statsBefore);
    }

}
