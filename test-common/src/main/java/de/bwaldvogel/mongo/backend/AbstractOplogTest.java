package de.bwaldvogel.mongo.backend;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;
import static de.bwaldvogel.mongo.backend.TestUtils.json;
import static de.bwaldvogel.mongo.backend.TestUtils.toArray;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.function.Consumer;

import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import de.bwaldvogel.mongo.oplog.OperationType;

public abstract class AbstractOplogTest extends AbstractTest {

    protected static final String LOCAL_DATABASE = "local";
    protected static final String OPLOG_COLLECTION_NAME = "oplog.rs";

    @Override
    protected void setUpBackend() throws Exception {
        super.setUpBackend();
        backend.enableOplog();

    }

    @Override
    protected void dropAllDatabases() {
        super.dropAllDatabases();
        clearOplog();
    }

    protected void clearOplog() {
        getOplogCollection().deleteMany(json(""));
    }

    protected MongoCollection<Document> getOplogCollection() {
        MongoDatabase localDb = syncClient.getDatabase(LOCAL_DATABASE);
        return localDb.getCollection(OPLOG_COLLECTION_NAME);
    }

    @Test
    public void testListDatabaseNames() throws Exception {
        assertThat(listDatabaseNames()).contains(LOCAL_DATABASE);
        collection.insertOne(json(""));
        assertThat(listDatabaseNames()).containsExactlyInAnyOrder(db.getName(), LOCAL_DATABASE);
        syncClient.getDatabase("bar").getCollection("some-collection").insertOne(json(""));
        assertThat(listDatabaseNames()).containsExactlyInAnyOrder("bar", db.getName(), LOCAL_DATABASE);
    }

    @Test
    public void testOplogInsertUpdateAndDelete() {
        Document document = json("_id: 1, name: 'testUser1'");

        collection.insertOne(document);
        clock.windForward(Duration.ofSeconds(1));
        collection.updateOne(json("_id: 1"), json("$set: {name: 'user 2'}"));
        clock.windForward(Duration.ofSeconds(1));
        collection.deleteOne(json("_id: 1"));

        List<Document> oplogDocuments = toArray(getOplogCollection().find().sort(json("ts: 1")));
        assertThat(oplogDocuments).hasSize(3);

        Document insertOplogDocument = oplogDocuments.get(0);
        assertThat(insertOplogDocument).containsKeys("ts", "t", "h", "v", "op", "ns", "ui", "wall", "o");
        assertThat(insertOplogDocument.get("ts")).isInstanceOf(BsonTimestamp.class);
        assertThat(insertOplogDocument.get("t")).isEqualTo(1L);
        assertThat(insertOplogDocument.get("h")).isEqualTo(0L);
        assertThat(insertOplogDocument.get("v")).isEqualTo(2L);
        assertThat(insertOplogDocument.get("op")).isEqualTo(OperationType.INSERT.getCode());
        assertThat(insertOplogDocument.get("ns")).isEqualTo(collection.getNamespace().getFullName());
        assertThat(insertOplogDocument.get("ui")).isInstanceOf(UUID.class);
        assertThat(insertOplogDocument.get("wall")).isEqualTo(Date.from(Instant.parse("2019-05-23T12:00:00.123Z")));
        assertThat(insertOplogDocument.get("o")).isEqualTo(document);

        Document updateOplogDocument = oplogDocuments.get(1);
        assertThat(updateOplogDocument).containsKeys("ts", "t", "h", "v", "op", "ns", "ui", "wall", "o", "o2");
        assertThat(updateOplogDocument.get("ts")).isInstanceOf(BsonTimestamp.class);
        assertThat(updateOplogDocument.get("t")).isEqualTo(1L);
        assertThat(updateOplogDocument.get("h")).isEqualTo(0L);
        assertThat(updateOplogDocument.get("v")).isEqualTo(2L);
        assertThat(updateOplogDocument.get("op")).isEqualTo(OperationType.UPDATE.getCode());
        assertThat(updateOplogDocument.get("ns")).isEqualTo(collection.getNamespace().getFullName());
        assertThat(updateOplogDocument.get("ui")).isInstanceOf(UUID.class);
        assertThat(updateOplogDocument.get("wall")).isEqualTo(Date.from(Instant.parse("2019-05-23T12:00:01.123Z")));
        assertThat(updateOplogDocument.get("o2")).isEqualTo(json("_id: 1"));
        assertThat(updateOplogDocument.get("o")).isEqualTo(json("$set: {name: 'user 2'}"));

        Document deleteOplogDocument = oplogDocuments.get(2);
        assertThat(deleteOplogDocument).containsKeys("ts", "t", "h", "v", "op", "ns", "ui", "wall", "o");
        assertThat(deleteOplogDocument.get("ts")).isInstanceOf(BsonTimestamp.class);
        assertThat(deleteOplogDocument.get("t")).isEqualTo(1L);
        assertThat(deleteOplogDocument.get("h")).isEqualTo(0L);
        assertThat(deleteOplogDocument.get("v")).isEqualTo(2L);
        assertThat(deleteOplogDocument.get("op")).isEqualTo(OperationType.DELETE.getCode());
        assertThat(deleteOplogDocument.get("ns")).isEqualTo(collection.getNamespace().getFullName());
        assertThat(deleteOplogDocument.get("ui")).isInstanceOf(UUID.class);
        assertThat(deleteOplogDocument.get("wall")).isEqualTo(Date.from(Instant.parse("2019-05-23T12:00:02.123Z")));
        assertThat(deleteOplogDocument.get("o")).isEqualTo(json("_id: 1"));
    }

    @Test
    public void testQueryOplogWhenOplogIsDisabled() throws Exception {
        backend.disableOplog();
        collection.insertOne(json("_id: 1"));
        assertThat(getOplogCollection().find()).isEmpty();
    }

    @Test
    @Disabled("This test represents a missing feature") //Todo. Support replace one.
    public void testSetOplogReplaceOneById() {
        collection.insertOne(json("_id: 1, b: 6"));
        collection.replaceOne(json("_id: 1"), json("a: 5, b: 7"));
        List<Document> oplogDocuments = toArray(getOplogCollection().find().sort(json("ts: 1")));
        Document updateOplogEntry = oplogDocuments.get(1);
        assertThat(updateOplogEntry.get("op")).isEqualTo(OperationType.UPDATE.getCode());
        assertThat(updateOplogEntry.get("ns")).isEqualTo(collection.getNamespace().toString());
        assertThat(updateOplogEntry.get("o")).isEqualTo(json("_id: 1, a: 5, b: 7"));
        assertThat(updateOplogEntry.get("o2")).isEqualTo(json("_id: 1"));
    }

    @Test
    public void testSetOplogUpdateOneById() {
        collection.insertOne(json("_id: 34, b: 6"));
        collection.updateOne(eq("_id", 34), set("a", 6));
        List<Document> oplogDocuments = toArray(getOplogCollection().find().sort(json("ts: 1")));

        Document updateOplogDocument = oplogDocuments.get(1);
        assertThat(updateOplogDocument).containsKeys("ts", "t", "h", "v", "op", "ns", "ui", "wall", "o", "o2");
        assertThat(updateOplogDocument.get("ts")).isInstanceOf(BsonTimestamp.class);
        assertThat(updateOplogDocument.get("t")).isEqualTo(1L);
        assertThat(updateOplogDocument.get("h")).isEqualTo(0L);
        assertThat(updateOplogDocument.get("v")).isEqualTo(2L);
        assertThat(updateOplogDocument.get("op")).isEqualTo(OperationType.UPDATE.getCode());
        assertThat(updateOplogDocument.get("ns")).isEqualTo(collection.getNamespace().getFullName());
        assertThat(updateOplogDocument.get("ui")).isInstanceOf(UUID.class);
        assertThat(updateOplogDocument.get("o2")).isEqualTo(json("_id: 34"));
        assertThat(updateOplogDocument.get("o")).isEqualTo(json("$set: {a: 6}"));
    }

    @Test
    @Disabled("This test represents a missing feature")
    public void testSetOplogUpdateOneByIdMultipleFields() {
        collection.insertOne(json("_id: 1, b: 6"));
        collection.updateOne(eq("_id", 1), Arrays.asList(set("a", 7), set("b", 7)));
        List<Document> oplogDocuments = toArray(getOplogCollection().find().sort(json("ts: 1")));

        Document updateOplogDocument = oplogDocuments.get(1);
        assertThat(updateOplogDocument).containsKeys("ts", "t", "h", "v", "op", "ns", "ui", "wall", "o", "o2");
        assertThat(updateOplogDocument.get("ts")).isInstanceOf(BsonTimestamp.class);
        assertThat(updateOplogDocument.get("t")).isEqualTo(1L);
        assertThat(updateOplogDocument.get("h")).isEqualTo(0L);
        assertThat(updateOplogDocument.get("v")).isEqualTo(2L);
        assertThat(updateOplogDocument.get("op")).isEqualTo(OperationType.UPDATE.getCode());
        assertThat(updateOplogDocument.get("ns")).isEqualTo(collection.getNamespace().getFullName());
        assertThat(updateOplogDocument.get("ui")).isInstanceOf(UUID.class);
        assertThat(updateOplogDocument.get("o2")).isEqualTo(json("_id: 1"));
        assertThat(updateOplogDocument.get("o")).isEqualTo(json("$set: {a: 7, b: 7}"));
    }

    @Test
    public void testSetOplogUpdateMany() {
        collection.insertMany(Arrays.asList(json("_id: 1, b: 6"), json("_id: 2, b: 6")));
        collection.updateMany(eq("b", 6), set("a", 7));

        List<Document> oplogDocuments = toArray(getOplogCollection().find().sort(json("ts: 1")));
        assertThat(oplogDocuments).hasSize(4);

        for (int i = 2; i < 4; i++) {
            Document updateOplogDocument = oplogDocuments.get(i);
            assertThat(updateOplogDocument).containsKeys("ts", "t", "h", "v", "op", "ns", "ui", "wall", "o", "o2");
            assertThat(updateOplogDocument.get("ts")).isInstanceOf(BsonTimestamp.class);
            assertThat(updateOplogDocument.get("t")).isEqualTo(1L);
            assertThat(updateOplogDocument.get("h")).isEqualTo(0L);
            assertThat(updateOplogDocument.get("v")).isEqualTo(2L);
            assertThat(updateOplogDocument.get("op")).isEqualTo(OperationType.UPDATE.getCode());
            assertThat(updateOplogDocument.get("ns")).isEqualTo(collection.getNamespace().getFullName());
            assertThat(updateOplogDocument.get("ui")).isInstanceOf(UUID.class);
            assertThat(updateOplogDocument.get("o2")).isEqualTo(json(String.format("_id: %d", i - 1)));
            assertThat(updateOplogDocument.get("o")).isEqualTo(json("$set: {a: 7}"));
        }
    }

    @Test
    public void testSetOplogDeleteMany() {
        collection.insertMany(Arrays.asList(json("_id: 1, b: 6"), json("_id: 2, b: 6")));
        collection.deleteMany(eq("b", 6));

        List<Document> oplogDocuments = toArray(getOplogCollection().find().sort(json("ts: 1")));
        assertThat(oplogDocuments).hasSize(4);

        for (int i = 2; i < 4; i++) {
            Document updateOplogDocument = oplogDocuments.get(i);
            assertThat(updateOplogDocument).containsKeys("ts", "t", "h", "v", "op", "ns", "ui", "wall", "o");
            assertThat(updateOplogDocument.get("ts")).isInstanceOf(BsonTimestamp.class);
            assertThat(updateOplogDocument.get("t")).isEqualTo(1L);
            assertThat(updateOplogDocument.get("h")).isEqualTo(0L);
            assertThat(updateOplogDocument.get("v")).isEqualTo(2L);
            assertThat(updateOplogDocument.get("op")).isEqualTo(OperationType.DELETE.getCode());
            assertThat(updateOplogDocument.get("ns")).isEqualTo(collection.getNamespace().getFullName());
            assertThat(updateOplogDocument.get("ui")).isInstanceOf(UUID.class);
            assertThat(updateOplogDocument.get("o")).isEqualTo(json(String.format("_id: %d", i - 1)));
        }
    }

}
