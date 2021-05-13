package de.bwaldvogel.mongo.backend;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;
import static com.mongodb.client.model.Updates.unset;
import static de.bwaldvogel.mongo.backend.TestUtils.json;
import static de.bwaldvogel.mongo.backend.TestUtils.toArray;
import static java.util.Collections.singletonList;
import static org.assertj.core.groups.Tuple.tuple;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.Success;

import de.bwaldvogel.mongo.oplog.OperationType;
import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subscribers.TestSubscriber;

public abstract class AbstractOplogTest extends AbstractTest {

    protected static final String LOCAL_DATABASE = "local";
    protected static final String OPLOG_COLLECTION_NAME = "oplog.rs";

    @BeforeEach
    public void beforeEach() {
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
        List<Document> oplogDocuments = toArray(getOplogCollection().find(json("op: 'u'")).sort(json("ts: 1")));

        Document updateOplogDocument = CollectionUtils.getSingleElement(oplogDocuments);
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

        List<Document> oplogDocuments = toArray(getOplogCollection().find(json("op: 'u'")).sort(json("ts: 1, 'o2._id': 1")));
        assertThat(oplogDocuments).hasSize(2);

        for (int i = 0; i < 2; i++) {
            Document updateOplogDocument = oplogDocuments.get(i);
            assertThat(updateOplogDocument).containsKeys("ts", "t", "h", "v", "op", "ns", "ui", "wall", "o", "o2");
            assertThat(updateOplogDocument.get("ts")).isInstanceOf(BsonTimestamp.class);
            assertThat(updateOplogDocument.get("t")).isEqualTo(1L);
            assertThat(updateOplogDocument.get("h")).isEqualTo(0L);
            assertThat(updateOplogDocument.get("v")).isEqualTo(2L);
            assertThat(updateOplogDocument.get("op")).isEqualTo(OperationType.UPDATE.getCode());
            assertThat(updateOplogDocument.get("ns")).isEqualTo(collection.getNamespace().getFullName());
            assertThat(updateOplogDocument.get("ui")).isInstanceOf(UUID.class);
            assertThat(updateOplogDocument.get("o2")).isEqualTo(json(String.format("_id: %d", i + 1)));
            assertThat(updateOplogDocument.get("o")).isEqualTo(json("$set: {a: 7}"));
        }
    }

    @Test
    public void testSetOplogDeleteMany() {
        collection.insertMany(Arrays.asList(json("_id: 1, b: 6"), json("_id: 2, b: 6")));
        collection.deleteMany(eq("b", 6));

        List<Document> oplogDocuments = toArray(getOplogCollection().find(json("op: 'd'")).sort(json("ts: 1, 'o._id': 1")));
        assertThat(oplogDocuments).hasSize(2);

        for (int i = 0; i < 2; i++) {
            Document updateOplogDocument = oplogDocuments.get(i);
            assertThat(updateOplogDocument).containsKeys("ts", "t", "h", "v", "op", "ns", "ui", "wall", "o");
            assertThat(updateOplogDocument.get("ts")).isInstanceOf(BsonTimestamp.class);
            assertThat(updateOplogDocument.get("t")).isEqualTo(1L);
            assertThat(updateOplogDocument.get("h")).isEqualTo(0L);
            assertThat(updateOplogDocument.get("v")).isEqualTo(2L);
            assertThat(updateOplogDocument.get("op")).isEqualTo(OperationType.DELETE.getCode());
            assertThat(updateOplogDocument.get("ns")).isEqualTo(collection.getNamespace().getFullName());
            assertThat(updateOplogDocument.get("ui")).isInstanceOf(UUID.class);
            assertThat(updateOplogDocument.get("o")).isEqualTo(json(String.format("_id: %d", i + 1)));
        }
    }

    @Test
    public void testChangeStreamInsertAndUpdateFullDocumentLookup() {
        collection.insertOne(json("b: 1"));
        int numberOfDocs = 10;
        List<Document> insert = new ArrayList<>();
        List<Document> update = new ArrayList<>();
        List<ChangeStreamDocument<Document>> changeStreamsResult = new ArrayList<>();
        List<Bson> pipeline = singletonList(match(Filters.or(
            Document.parse("{'fullDocument.b': 1}")))
        );

        try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor =
                 collection.watch(pipeline).fullDocument(FullDocument.UPDATE_LOOKUP).cursor()) {

            final long cursorId = cursor.getServerCursor().getId();

            for (int i = 1; i < numberOfDocs + 1; i++) {
                Document doc = json(String.format("a: %d, b: 1", i));
                collection.insertOne(doc);
                collection.updateOne(eq("a", i), set("c", i * 10));

                assertThat(cursor.hasNext()).isTrue();
                ChangeStreamDocument<Document> insertDocument = cursor.next();
                assertThat(cursor.getServerCursor().getId()).isEqualTo(cursorId);

                assertThat(cursor.hasNext()).isTrue();
                ChangeStreamDocument<Document> updateDocument = cursor.next();
                assertThat(cursor.getServerCursor().getId()).isEqualTo(cursorId);

                assertThat(insertDocument.getFullDocument().get("a")).isEqualTo(i);
                insert.add(insertDocument.getFullDocument());

                assertThat(updateDocument.getFullDocument().get("a")).isEqualTo(i);
                update.add(updateDocument.getFullDocument());

                changeStreamsResult.addAll(Arrays.asList(insertDocument, updateDocument));
            }
        }

        assertThat(insert.size()).isEqualTo(numberOfDocs);
        assertThat(update.size()).isEqualTo(numberOfDocs);
        assertThat(changeStreamsResult.size()).isEqualTo(numberOfDocs * 2);
    }

    @Test
    public void testChangeStreamUpdateDefault() {
        collection.insertOne(json("a: 1, b: 2, c: 3"));
        try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = collection.watch().cursor()) {
            collection.updateOne(eq("a", 1), json("$set: {b: 0, c: 10}"));
            ChangeStreamDocument<Document> updateDocument = cursor.next();
            Document fullDoc = updateDocument.getFullDocument();
            assertThat(fullDoc).isNotNull();
            assertThat(fullDoc.get("b")).isEqualTo(0);
            assertThat(fullDoc.get("c")).isEqualTo(10);

            collection.updateOne(eq("a", 1), unset("b"));
            updateDocument = cursor.next();
            fullDoc = updateDocument.getFullDocument();
            assertThat(fullDoc).isNotNull();
            assertThat(fullDoc.get("b")).isEqualTo("");
        }
    }

    @Test
    public void testChangeStreamDelete() {
        collection.insertOne(json("_id: 1"));
        try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = collection.watch().cursor()) {
            collection.deleteOne(json("_id: 1"));
            ChangeStreamDocument<Document> deleteDocument = cursor.next();
            assertThat(deleteDocument.getDocumentKey().get("_id")).isEqualTo(new BsonInt32(1));
        }
    }

    @Test
    public void testChangeStreamStartAfter() {
        collection.insertOne(json("a: 1")); // This is needed to initialize the collection in the server.
        try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = collection.watch().cursor()) {
            collection.insertOne(json("a: 2"));
            collection.insertOne(json("a: 3"));
            ChangeStreamDocument<Document> document = cursor.next();
            BsonDocument resumeToken = document.getResumeToken();

            try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor2
                     = collection.watch().startAfter(resumeToken).cursor()) {
                ChangeStreamDocument<Document> document2 = cursor2.next();
                assertThat(document2.getFullDocument().get("a")).isEqualTo(3);
            }
        }
    }

    @Test
    public void testChangeStreamResumeAfter() {
        collection.insertOne(json("a: 1"));
        try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = collection.watch().cursor();) {
            collection.insertOne(json("a: 2"));
            collection.insertOne(json("a: 3"));
            ChangeStreamDocument<Document> document = cursor.next();
            BsonDocument resumeToken = document.getResumeToken();

            try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor2
                     = collection.watch().resumeAfter(resumeToken).cursor()) {
                ChangeStreamDocument<Document> document2 = cursor2.next();
                assertThat(document2.getFullDocument().get("a")).isEqualTo(3);
            }
        }
    }

    @Test
    public void testChangeStreamResumeAfterTerminalEvent() {
        MongoCollection<Document> col = db.getCollection("test-collection");
        ChangeStreamIterable<Document> watch = col.watch().fullDocument(FullDocument.UPDATE_LOOKUP).batchSize(1);
        try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = watch.cursor()) {
            col.insertOne(json("a: 1"));
            cursor.next();

            col.drop();

            ChangeStreamDocument<Document> document = cursor.next();
            BsonDocument resumeToken = document.getResumeToken();
            try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> resumeAfterCursor
                     = watch.resumeAfter(resumeToken).cursor();) {
                document = resumeAfterCursor.next();

                assertThat(document).isNotNull();
                assertThat(document.getOperationType())
                    .isEqualTo(com.mongodb.client.model.changestream.OperationType.INVALIDATE);

                assertThatExceptionOfType(NoSuchElementException.class)
                    .isThrownBy(resumeAfterCursor::next);
            }
        }
    }

    @Test
    public void testChangeStreamStartAtOperationTime() {
        collection.insertOne(json("a: 1"));
        try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = collection.watch().cursor()) {
            collection.insertOne(json("a: 2"));
            collection.insertOne(json("a: 3"));
            ChangeStreamDocument<Document> document = cursor.next();
            BsonTimestamp startAtOperationTime = document.getClusterTime();

            MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor2 = collection.watch().startAtOperationTime(startAtOperationTime).cursor();
            ChangeStreamDocument<Document> document2 = cursor2.next();
            assertThat(document2.getFullDocument().get("a")).isEqualTo(2);
            document2 = cursor2.next();
            assertThat(document2.getFullDocument().get("a")).isEqualTo(3);
        }
    }

    @Test
    public void testChangeStreamAndReplaceOneWithUpsertTrue() throws Exception {
        TestSubscriber<ChangeStreamDocument<Document>> streamSubscriber = new TestSubscriber<>();
        asyncCollection.watch().fullDocument(FullDocument.UPDATE_LOOKUP).subscribe(streamSubscriber);

        TestSubscriber<UpdateResult> replaceOneSubscriber = new TestSubscriber<>();
        asyncCollection.replaceOne(json("a: 1"), json("a: 1"), new ReplaceOptions().upsert(true))
            .subscribe(replaceOneSubscriber);

        replaceOneSubscriber.awaitTerminalEvent();
        replaceOneSubscriber.assertNoErrors();

        TestSubscriber<Document> findSubscriber = new TestSubscriber<>();
        asyncCollection.find(json("a:1")).subscribe(findSubscriber);
        findSubscriber.awaitTerminalEvent();
        assertThat(getSingleValue(findSubscriber).get("a")).isEqualTo(1);

        streamSubscriber.awaitCount(1).assertValueCount(1).cancel();
        assertThat(getSingleValue(streamSubscriber).getOperationType().getValue()).isEqualTo("insert");
        assertThat(getSingleValue(streamSubscriber).getFullDocument()).isEqualTo(getSingleValue(findSubscriber));
    }

    @Test
    public void testSimpleChangeStreamWithFilter() throws Exception {
        TestSubscriber<Success> insertSubscriber1 = new TestSubscriber<>();
        TestSubscriber<Success> insertSubscriber2 = new TestSubscriber<>();
        TestSubscriber<ChangeStreamDocument<Document>> streamSubscriber = new TestSubscriber<>();

        asyncCollection.insertOne(json("_id: 2")).subscribe(insertSubscriber1);
        insertSubscriber1.awaitTerminalEvent();

        Bson filter = match(Filters.eq("fullDocument.bu", "abc"));
        List<Bson> pipeline = singletonList(filter);

        asyncCollection.watch(pipeline).subscribe(streamSubscriber);

        insertSubscriber1 = new TestSubscriber<>();
        asyncCollection.insertOne(json("_id: 2, bu: 'abc'")).subscribe(insertSubscriber1);
        asyncCollection.insertOne(json("_id: 3, bu: 'xyz'")).subscribe(insertSubscriber2);
        insertSubscriber1.awaitTerminalEvent();
        insertSubscriber2.awaitTerminalEvent();

        streamSubscriber.awaitCount(1).assertValueCount(1).cancel();
        assertThat(getSingleValue(streamSubscriber).getFullDocument().get("bu")).isEqualTo("abc");
    }

    @Test
    public void testOplogShouldFilterNamespaceOnChangeStreams() throws Exception {
        TestSubscriber<Success> insertSubscriber = new TestSubscriber<>();
        com.mongodb.reactivestreams.client.MongoCollection<Document> asyncCollection1 =
            asyncDb.getCollection(asyncCollection.getNamespace().getCollectionName() + "1");

        asyncCollection.insertOne(json("_id: 1")).subscribe(insertSubscriber);
        asyncCollection1.insertOne(json("_id: 1")).subscribe(insertSubscriber);

        insertSubscriber.awaitTerminalEvent(1, TimeUnit.SECONDS);

        TestSubscriber<ChangeStreamDocument<Document>> streamSubscriber = new TestSubscriber<>();
        Bson filter = match(Filters.eq("fullDocument.a", 1));
        asyncCollection.watch(singletonList(filter)).subscribe(streamSubscriber);

        // Necessary to give time for the change stream to start before the below insert operation is executed.
        Thread.sleep(50);

        insertSubscriber = new TestSubscriber<>();
        asyncCollection1.insertOne(json("_id: 2, a: 1")).subscribe(insertSubscriber);
        insertSubscriber.awaitTerminalEvent(1, TimeUnit.SECONDS);

        streamSubscriber.awaitTerminalEvent(1, TimeUnit.SECONDS);
        assertThat(streamSubscriber.values()).isEmpty();
    }

    private static <T> T getSingleValue(TestSubscriber<T> subscriber) {
        return subscriber.values().get(0);
    }

    @Test
    public void testMultipleChangeStreams() throws InterruptedException {
        Flowable.fromPublisher(asyncCollection.insertOne(json("_id: 1")))
            .test().awaitDone(5, TimeUnit.SECONDS).assertComplete();

        final int changeStreamCount = 32;

        List<Bson> pipeline = singletonList(match(Filters.eq("fullDocument.bu", "abc")));

        final TestSubscriber<Map<Integer, List<ChangeStreamDocument<Document>>>> streamSubscriber
            = new TestSubscriber<>();

        Flowable.range(1, changeStreamCount)
            .flatMapSingle(index -> {
                return Flowable.fromPublisher(asyncCollection.watch(pipeline))
                    .take(2)
                    .toList()
                    .map(changeStreamDocuments -> {
                        return new AbstractMap.SimpleEntry<>(index, changeStreamDocuments);
                    })
                    .subscribeOn(Schedulers.io()); // subscribe to change streams concurrently
            })
            .toMap(Map.Entry::getKey, Map.Entry::getValue)
            .toFlowable()
            .subscribe(streamSubscriber);

        // give time for all ChangeStream Publishers to be subscribed to
        // todo: expose API to get cursors from Backend and wait until 'changeStreamCount' cursors
        TimeUnit.SECONDS.sleep(5);

        Flowable.concat(
            Flowable.fromPublisher(asyncCollection.insertOne(json("_id: 2, bu: 'abc'"))),
            Flowable.fromPublisher(asyncCollection.insertOne(json("_id: 3, bu: 'xyz'"))),
            Flowable.fromPublisher(asyncCollection.insertOne(json("_id: 4, bu: 'abc'")))
        ).test().awaitDone(15, TimeUnit.SECONDS).assertComplete();

        final Map<Integer, List<ChangeStreamDocument<Document>>> results = streamSubscriber
            .awaitDone(30, TimeUnit.SECONDS)
            .assertComplete()
            .assertValueCount(1)
            .values().get(0);

        Assertions.assertThat(IntStream.rangeClosed(1, changeStreamCount))
            .allSatisfy(index -> {
                Assertions.assertThat(results).containsKey(index);

                final List<ChangeStreamDocument<Document>> emits = results.get(index);
                Assertions.assertThat(emits).isNotNull()
                    .extracting(
                        document -> {
                            return document.getDocumentKey().getInt32("_id").getValue();
                        },
                        document -> {
                            return document.getFullDocument() != null
                                ? document.getFullDocument().getString("bu")
                                : null;
                        }
                    )
                    .containsExactly(tuple(2, "abc"), tuple(4, "abc"));
            });
    }

}
