package de.bwaldvogel.mongo.backend;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.ne;
import static com.mongodb.client.model.Filters.nor;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Updates.addEachToSet;
import static com.mongodb.client.model.Updates.pull;
import static com.mongodb.client.model.Updates.pullByFilter;
import static com.mongodb.client.model.Updates.set;
import static de.bwaldvogel.mongo.backend.TestUtils.date;
import static de.bwaldvogel.mongo.backend.TestUtils.getCollectionStatistics;
import static de.bwaldvogel.mongo.backend.TestUtils.instant;
import static de.bwaldvogel.mongo.backend.TestUtils.json;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.bson.BsonInt32;
import org.bson.BsonJavaScript;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import com.mongodb.DBRef;
import com.mongodb.DuplicateKeyException;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoCursorNotFoundException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoQueryException;
import com.mongodb.MongoServerException;
import com.mongodb.MongoWriteException;
import com.mongodb.ReadPreference;
import com.mongodb.ServerCursor;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.BulkWriteUpsert;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.DeleteManyModel;
import com.mongodb.client.model.EstimatedDocumentCountOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.RenameCollectionOptions;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;

public abstract class AbstractBackendTest extends AbstractTest {

    private static final Logger log = LoggerFactory.getLogger(AbstractBackendTest.class);

    protected static final String OTHER_TEST_DATABASE_NAME = "bar";
    private static final Duration DEFAULT_TEST_TIMEOUT = Duration.ofSeconds(30);

    protected MongoCollection<Document> getCollection(String collectionName) {
        return db.getCollection(collectionName);
    }

    private String getCollectionName() {
        return collection.getNamespace().getCollectionName();
    }

    @Test
    void testSimpleInsert() {
        collection.insertOne(json("_id: 1"));
    }

    @Test
    void testSimpleCursor() {
        int expectedCount = 20;
        int batchSize = 10;
        for (int i = 0; i < expectedCount; i++) {
            collection.insertOne(new Document("_id", 100 + i));
        }
        MongoCursor<Document> cursor = collection.find().sort(json("_id: 1")).batchSize(batchSize).cursor();
        List<Document> retrievedDocuments = new ArrayList<>();
        while (cursor.hasNext()) {
            retrievedDocuments.add(cursor.next());
        }

        assertThatExceptionOfType(NoSuchElementException.class)
            .isThrownBy(cursor::next)
            .withMessage(null);

        assertThat(retrievedDocuments).hasSize(expectedCount);
        assertThat(retrievedDocuments).first().isEqualTo(json("_id: 100"));
        assertThat(retrievedDocuments).last().isEqualTo(json("_id: 119"));
    }

    @Test
    void testCursor_skipDocuments() {
        int totalCount = 20;
        int numToSkip = 5;
        int expectedCount = totalCount - numToSkip;
        int batchSize = 10;
        for (int i = 0; i < totalCount; i++) {
            collection.insertOne(new Document("_id", 100 + i));
        }
        MongoCursor<Document> cursor = collection.find()
            .sort(json("_id: 1"))
            .skip(numToSkip)
            .batchSize(batchSize)
            .cursor();
        List<Document> retrievedDocuments = new ArrayList<>();
        while (cursor.hasNext()) {
            retrievedDocuments.add(cursor.next());
        }

        assertThatExceptionOfType(NoSuchElementException.class)
            .isThrownBy(cursor::next)
            .withMessage(null);

        assertThat(retrievedDocuments).hasSize(expectedCount);
        assertThat(retrievedDocuments).first().isEqualTo(json("_id: 105"));
        assertThat(retrievedDocuments).last().isEqualTo(json("_id: 119"));
    }

    @Test
    void testCursor_skipAndLimitDocuments() {
        int totalCount = 50;
        int numToSkip = 5;
        int limit = 20;
        int batchSize = 10;
        for (int i = 0; i < totalCount; i++) {
            collection.insertOne(new Document("_id", 100 + i));
        }
        MongoCursor<Document> cursor = collection.find()
            .sort(json("_id: 1"))
            .skip(numToSkip)
            .limit(limit)
            .batchSize(batchSize)
            .cursor();

        List<Document> retrievedDocuments = new ArrayList<>();
        while (cursor.hasNext()) {
            retrievedDocuments.add(cursor.next());
        }

        assertThatExceptionOfType(NoSuchElementException.class)
            .isThrownBy(cursor::next)
            .withMessage(null);

        assertThat(retrievedDocuments).hasSize(limit);
        assertThat(retrievedDocuments).first().isEqualTo(json("_id: 105"));
        assertThat(retrievedDocuments).last().isEqualTo(json("_id: 124"));
    }

    @Test
    void testCursor_withProjection() {
        int totalCount = 50;
        int numToSkip = 5;
        int limit = 20;
        int batchSize = 10;
        for (int i = 0; i < totalCount; i++) {
            collection.insertOne(new Document("_id", 100 + i).append("x", 1000 + i));
        }
        MongoCursor<Document> cursor = collection.find()
            .sort(json("_id: 1"))
            .skip(numToSkip)
            .limit(limit)
            .batchSize(batchSize)
            .projection(json("_id: 0, x: 1"))
            .cursor();

        List<Document> retrievedDocuments = new ArrayList<>();
        while (cursor.hasNext()) {
            retrievedDocuments.add(cursor.next());
        }

        assertThatExceptionOfType(NoSuchElementException.class)
            .isThrownBy(cursor::next)
            .withMessage(null);

        assertThat(retrievedDocuments).hasSize(limit);
        assertThat(retrievedDocuments).first().isEqualTo(json("x: 1005"));
        assertThat(retrievedDocuments).last().isEqualTo(json("x: 1024"));
    }

    @Test
    void testCloseCursor() {
        int expectedCount = 20;
        int batchSize = 5;
        for (int i = 0; i < expectedCount; i++) {
            collection.insertOne(new Document("value", i));
        }
        MongoCursor<Document> cursor = collection.find().batchSize(batchSize).cursor();
        int count = 0;
        while (cursor.hasNext() && count < 10) {
            cursor.next();
            count++;
        }
        cursor.close();
        assertThat(count).isEqualTo(10);
        assertThatExceptionOfType(IllegalStateException.class)
            .isThrownBy(cursor::next)
            .withMessage("Cursor has been closed");
    }

    @Test
    public void testCursor_iteratingACursorThatNoLongerExists() {
        int expectedCount = 20;
        for (int i = 0; i < expectedCount; i++) {
            collection.insertOne(new Document("name", "testUser1"));
        }
        MongoCursor<Document> cursor = collection.find().batchSize(1).cursor();
        cursor.next();
        killCursors(List.of(cursor.getServerCursor().getId()));

        assertThatExceptionOfType(MongoCursorNotFoundException.class)
            .isThrownBy(cursor::next)
            .withMessageMatching("Command execution failed on MongoDB server with error 43 \\(CursorNotFound\\): 'Cursor id \\d+ does not exist'.+");
    }

    @Test
    void testKillCursor() {
        for (int i = 0; i < 20; i++) {
            collection.insertOne(json(""));
        }
        MongoCursor<Document> cursor = collection.find().batchSize(1).cursor();
        ServerCursor serverCursor = cursor.getServerCursor();
        String collectionName = collection.getNamespace().getCollectionName();
        Document result = runCommand(new Document("killCursors", collectionName).append("cursors", List.of(serverCursor.getId())));
        assertThat(result.getDouble("ok")).isEqualTo(1.0);
        assertThat(result.get("cursorsKilled")).isEqualTo(List.of(serverCursor.getId()));
        assertThat(result.get("cursorsNotFound")).isEqualTo(Collections.emptyList());
    }

    @Test
    void testKillCursor_unknownCursorId() {
        collection.insertOne(json(""));
        String collectionName = collection.getNamespace().getCollectionName();
        Document result = runCommand(new Document("killCursors", collectionName).append("cursors", List.of(987654321L)));
        assertThat(result.getDouble("ok")).isEqualTo(1.0);
        assertThat(result.get("cursorsKilled")).isEqualTo(Collections.emptyList());
        assertThat(result.get("cursorsNotFound")).isEqualTo(List.of(987654321L));
    }

    @Test
    void testSimpleInsertDelete() {
        collection.insertOne(json("_id: 1"));
        collection.deleteOne(json("_id: 1"));
    }

    @Test
    void testCreateCollection() {
        String newCollectionName = "some-collection";
        assertThat(db.listCollectionNames()).doesNotContain(newCollectionName);
        db.createCollection(newCollectionName, new CreateCollectionOptions());
        assertThat(db.listCollectionNames()).contains(newCollectionName);
    }

    @Test
    void testCreateCappedCollection_invalidOptions() {
        String newCollectionName = "some-collection";
        assertThat(db.listCollectionNames()).doesNotContain(newCollectionName);

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> db.createCollection("some-collection", new CreateCollectionOptions().capped(true)))
            .withMessageStartingWith("Command execution failed on MongoDB server with error 72 (InvalidOptions): 'the 'size' field is required when 'capped' is true'");
    }

    @Test
    void testCreateCollectionAlreadyExists() {
        db.createCollection("some-collection", new CreateCollectionOptions());

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> db.createCollection("some-collection", new CreateCollectionOptions()))
            .withMessageStartingWith("Command execution failed on MongoDB server with error 48 (NamespaceExists): 'Collection already exists. NS: testdb.some-collection'");
    }

    @Test
    void testUnsupportedModifier() {
        collection.insertOne(json(""));
        assertMongoWriteException(() -> collection.updateOne(json(""), json("$foo: {}")),
            9, "FailedToParse", "Unknown modifier: $foo. Expected a valid update modifier or pipeline-style update specified as an array");
    }

    @Test
    void testUpsertWithInc() {
        Document query = json("_id: {f: 'ca', '1': {l: 2}, t: {t: 11}}");
        Document update = json("'$inc': {'n.!' : 1 , 'n.a.b:false' : 1}");

        collection.updateOne(query, update, new UpdateOptions().upsert(true));

        query.putAll(json("n: {'!': 1, a: {'b:false': 1}}"));
        assertThat(collection.find().first()).isEqualTo(query);
    }

    @Test
    void testBasicUpdate() {
        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2, b: 5"));
        collection.insertOne(json("_id: 3"));
        collection.insertOne(json("_id: 4"));

        collection.replaceOne(json("_id: 2"), json("_id: 2, a: 5"));

        assertThat(collection.find(json("_id: 2")).first()).isEqualTo(json("_id: 2, a: 5"));
    }

    @Test
    void testCollectionStats_newCollection() {
        Document stats = getCollStats();
        assertThat(stats.getDouble("ok")).isEqualTo(1.0);
        assertThat(stats.getInteger("count")).isEqualTo(0);
        assertThat(stats.getInteger("size")).isEqualTo(0);
        assertThat(stats).doesNotContainKey("avgObjSize");

        assertThat(db.listCollectionNames()).doesNotContain(getCollectionName());
    }

    @Test
    void testCollectionStats() {
        collection.insertOne(json(""));
        collection.insertOne(json("abc: 'foo'"));

        Document stats = getCollStats();
        assertThat(stats.getDouble("ok")).isEqualTo(1.0);
        assertThat(stats.getInteger("count")).isEqualTo(2);
        assertThat(stats.getInteger("size")).isEqualTo(57);
        assertThat(stats.getInteger("avgObjSize")).isEqualTo(28);
    }

    private Document getCollStats() {
        String collectionName = getCollectionName();
        return getCollectionStatistics(db, collectionName);
    }

    @Test
    public void testGetLogStartupWarnings() {
        Document startupWarnings = runCommand(json("getLog: 'startupWarnings'"));
        assertThat(startupWarnings.getDouble("ok")).isEqualTo(1.0);
        assertThat(startupWarnings.get("totalLinesWritten")).isInstanceOf(Number.class);
        assertThat(startupWarnings.get("log")).isEqualTo(Collections.emptyList());
    }

    @Test
    void testGetLogWhichDoesNotExist() {
        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> runCommand(json("getLog: 'illegal'")))
            .withMessageStartingWith("Command execution failed on MongoDB server with error -1: 'no RamLog named: illegal'");
    }

    @Test
    void testCompoundDateIdUpserts() {
        Document query = json("_id: {$lt: {n: 'a', t: 10}, $gte: {n: 'a', t: 1}}");

        List<Document> toUpsert = List.of(
            json("_id: {n: 'a', t: 1}"),
            json("_id: {n: 'a', t: 2}"),
            json("_id: {n: 'a', t: 3}"),
            json("_id: {n: 'a', t: 11}"));

        for (Document dbo : toUpsert) {
            collection.replaceOne(dbo, new Document(dbo).append("foo", "bar"), new ReplaceOptions().upsert(true));
        }
        assertThat(collection.find(query))
            .containsExactly(
                json("_id: {n: 'a', t: 1}, foo: 'bar'"),
                json("_id: {n: 'a', t: 2}, foo: 'bar'"),
                json("_id: {n: 'a', t: 3}, foo: 'bar'")
            );
    }

    @Test
    void testCompoundSort() {
        collection.insertOne(json("a:1, _id: 1"));
        collection.insertOne(json("a:2, _id: 5"));
        collection.insertOne(json("a:1, _id: 2"));
        collection.insertOne(json("a:2, _id: 4"));
        collection.insertOne(json("a:1, _id: 3"));

        assertThat(collection.find().sort(json("a:1, _id: -1")))
            .containsExactly(
                json("a: 1, _id: 3"),
                json("a: 1, _id: 2"),
                json("a: 1, _id: 1"),
                json("a: 2, _id: 5"),
                json("a: 2, _id: 4")
            );
    }

    @Test
    void testCountCommand() {
        assertThat(db.runCommand(new Document("count", getCollectionName())))
            .isEqualTo(json("ok: 1.0, n: 0"));

        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));
        collection.insertOne(json("_id: 3"));

        assertThat(db.runCommand(new Document("count", getCollectionName()).append("query", json("_id: 2"))))
            .isEqualTo(json("ok: 1.0, n: 1"));

        assertThat(db.runCommand(new Document("count", getCollectionName()).append("query", json(""))))
            .isEqualTo(json("ok: 1.0, n: 3"));

        assertThat(db.runCommand(new Document("count", getCollectionName()).append("query", json("_id: 4"))))
            .isEqualTo(json("ok: 1.0, n: 0"));

        assertThat(db.runCommand(new Document("count", getCollectionName()).append("maxTimeMS", 5000)))
            .isEqualTo(json("ok: 1.0, n: 3"));
    }

    @Test
    void testNonPrimaryCountCommand() {
        assertThat(collection.withReadPreference(ReadPreference.nearest()).countDocuments()).isZero();
    }

    @Test
    @SuppressWarnings("deprecation")
    void testCountCommandWithQuery() {
        collection.insertOne(json("n:1"));
        collection.insertOne(json("n:2"));
        collection.insertOne(json("n:2"));
        assertThat(collection.countDocuments(json("n:2"))).isEqualTo(2);
    }

    @Test
    @SuppressWarnings("deprecation")
    void testCountCommandWithSkipAndLimit() {
        collection.insertOne(json("x: 1"));
        collection.insertOne(json("x: 1"));
        collection.insertOne(json("x: 2"));
        collection.insertOne(json("x: 1"));
        collection.insertOne(json("x: 2"));
        collection.insertOne(json("x: 1"));

        assertThat(collection.countDocuments(json("x: 1"), new CountOptions().skip(4).limit(2))).isEqualTo(0);
        assertThat(collection.countDocuments(json("x: 1"), new CountOptions().limit(3))).isEqualTo(3);
        assertThat(collection.countDocuments(json("x: 1"), new CountOptions().limit(10))).isEqualTo(4);
        assertThat(collection.countDocuments(json("x: 1"), new CountOptions().skip(1))).isEqualTo(3);
    }

    @Test
    void testCountDocuments() {
        assertThat(collection.countDocuments()).isZero();
    }

    @Test
    void testCountDocumentsWithQuery() {
        collection.insertOne(json("n:1"));
        collection.insertOne(json("n:2"));
        collection.insertOne(json("n:2"));
        assertThat(collection.countDocuments(json("n:2"))).isEqualTo(2);
    }

    @Test
    void testEstimatedDocumentCount() {
        assertThat(collection.estimatedDocumentCount()).isEqualTo(0);
        collection.insertOne(json("n:1"));
        collection.insertOne(json("n:2"));
        collection.insertOne(json("n:2"));
        assertThat(collection.estimatedDocumentCount()).isEqualTo(3);
        assertThat(collection.estimatedDocumentCount(new EstimatedDocumentCountOptions().maxTime(1, TimeUnit.SECONDS))).isEqualTo(3);
    }

    @Test
    void testCreateIndexes() {
        collection.createIndex(new Document("n", 1));
        collection.createIndex(new Document("b", 1));

        assertThat(collection.listIndexes())
            .containsExactlyInAnyOrder(
                json("key: {_id: 1}").append("name", "_id_").append("v", 2),
                json("key: {n: 1}").append("name", "n_1").append("v", 2),
                json("key: {b: 1}").append("name", "b_1").append("v", 2)
            );
    }

    @Test
    void testCreateIndexesWithoutNamespace() {
        collection.insertOne(json("_id: 1, b: 1"));

        Document result = db.runCommand(json("createIndexes: 'testcoll', indexes: [{key: {b: 1}, name: 'b_1'}]"));
        assertThat(result.getDouble("ok")).isEqualTo(1.0);

        assertThat(collection.listIndexes())
            .containsExactlyInAnyOrder(
                json("key: {_id: 1}").append("name", "_id_").append("v", 2),
                json("key: {b: 1}").append("name", "b_1").append("v", 2)
            );
    }

    @Test
    void testCreateSecondPrimaryKeyIndex() {
        collection.insertOne(json("_id: 1, b: 1"));

        Document result = db.runCommand(json("createIndexes: 'testcoll', indexes: [{key: {_id: 1}, name: '_id_1'}]"));
        assertThat(result.getDouble("ok")).isEqualTo(1.0);

        assertThat(collection.listIndexes())
            .containsExactlyInAnyOrder(
                json("key: {_id: 1}").append("name", "_id_").append("v", 2)
            );

        db.drop();
    }

    @Test
    void testCreateIndexOnNonExistingDatabase() {
        db.drop();

        Document result = db.runCommand(json("createIndexes: 'sometable', indexes: [{key: {_id: 1}, name: '_id_1'}]"));
        assertThat(result.getDouble("ok")).isEqualTo(1.0);

        db.drop();
    }

    @Test
    void testDropAndRecreateIndex() {
        collection.createIndex(new Document("n", 1));
        collection.createIndex(new Document("b", 1));
        collection.createIndex(new Document("c", 1), new IndexOptions().unique(true));

        collection.dropIndex(new Document("n", 1));

        collection.insertOne(json("_id: 1, c: 10"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.dropIndex(new Document("n", 1)))
            .withMessageStartingWith("Command execution failed on MongoDB server with error 27 (IndexNotFound): 'can't find index with key: { n: 1 }'");

        assertMongoWriteException(() -> collection.insertOne(json("_id: 2, c: 10")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: c_1 dup key: { c: 10 }");

        collection.dropIndex(new Document("c", 1));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.dropIndex(new Document("c", 1)))
            .withMessageStartingWith("Command execution failed on MongoDB server with error 27 (IndexNotFound): 'can't find index with key: { c: 1 }'");

        assertThat(collection.listIndexes())
            .containsExactlyInAnyOrder(
                json("key: {_id: 1}").append("name", "_id_").append("v", 2),
                json("key: {b: 1}").append("name", "b_1").append("v", 2)
            );

        collection.insertOne(json("_id: 2, c: 10"));

        assertThatExceptionOfType(DuplicateKeyException.class)
            .isThrownBy(() -> collection.createIndex(new Document("c", 1), new IndexOptions().unique(true)))
            .withMessageMatching("Write failed with error code 11000 and error message " +
                "'Index build failed: [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}: " +
                "Collection testdb\\.testcoll \\( [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12} \\) :: caused by :: " +
                "E11000 duplicate key error collection: testdb\\.testcoll index: c_1 dup key: .+(\n.+)?'");

        collection.deleteOne(json("_id: 1"));
        collection.createIndex(new Document("c", 1), new IndexOptions().unique(true));

        assertThat(collection.listIndexes())
            .containsExactlyInAnyOrder(
                json("key: {_id: 1}").append("name", "_id_").append("v", 2),
                json("key: {b: 1}").append("name", "b_1").append("v", 2),
                json("key: {c: 1}").append("name", "c_1").append("unique", true).append("v", 2)
            );
    }

    @Test
    void testDropIndexes() {
        collection.insertOne(json("_id: 1, c: 10"));

        collection.createIndex(new Document("c", 1), new IndexOptions().unique(true));

        assertThat(collection.listIndexes())
            .containsExactlyInAnyOrder(
                json("key: {_id: 1}").append("name", "_id_").append("v", 2),
                json("key: {c: 1}").append("name", "c_1").append("unique", true).append("v", 2)
            );

        collection.dropIndexes();

        assertThat(collection.listIndexes())
            .containsExactlyInAnyOrder(
                json("key: {_id: 1}").append("name", "_id_").append("v", 2)
            );

        collection.dropIndexes();
        collection.drop();
        collection.dropIndexes();
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/184
    @Test
    void testDropIndex_string() {
        collection.insertOne(json("_id: 1, c: 10"));

        String indexName = collection.createIndex(new Document("c", 1), new IndexOptions().unique(true));

        assertThat(collection.listIndexes()).hasSize(2);

        collection.dropIndex(indexName);

        assertThat(collection.listIndexes()).hasSize(1);

        collection.insertOne(json("_id: 2, c: 10"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.dropIndex(indexName))
            .withMessageStartingWith("Command execution failed on MongoDB server with error 27 (IndexNotFound): 'index not found with name [c_1]'");
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/184
    @Test
    void testDropIndex_null() {
        collection.insertOne(json("_id: 1, c: 10"));

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> collection.dropIndex((String) null))
            .withMessage("indexName can not be null");
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/171
    @Test
    void testDropIndexes_twoIndexesWithTheSameKey() {
        collection.insertOne(json("_id: 1, c: 10"));

        MongoCollection<Document> otherCollection = getCollection("other");
        otherCollection.insertOne(json("_id: 1, c: 10"));

        collection.createIndex(new Document("c", 1));
        otherCollection.createIndex(new Document("c", 1));

        assertThat(collection.listIndexes())
            .containsExactlyInAnyOrder(
                json("key: {_id: 1}").append("name", "_id_").append("v", 2),
                json("key: {c: 1}").append("name", "c_1").append("v", 2)
            );

        assertThat(otherCollection.listIndexes())
            .containsExactlyInAnyOrder(
                json("key: {_id: 1}").append("name", "_id_").append("v", 2),
                json("key: {c: 1}").append("name", "c_1").append("v", 2)
            );

        collection.dropIndex(json("c: 1"));

        assertThat(collection.listIndexes())
            .containsExactlyInAnyOrder(
                json("key: {_id: 1}").append("name", "_id_").append("v", 2)
            );

        assertThat(otherCollection.listIndexes())
            .containsExactlyInAnyOrder(
                json("key: {_id: 1}").append("name", "_id_").append("v", 2),
                json("key: {c: 1}").append("name", "c_1").append("v", 2)
            );
    }

    @Test
    public void testCurrentOperations() {
        Document currentOperations = getAdminDb().getCollection("$cmd.sys.inprog").find().first();
        assertThat(currentOperations).isNotNull();
        assertThat(currentOperations.get("inprog")).isInstanceOf(List.class);
    }

    @Test
    void testListCollectionsEmpty() {
        Document result = db.runCommand(json("listCollections: 1"));
        assertThat(result.getDouble("ok")).isEqualTo(1.0);
        Document cursor = (Document) result.get("cursor");
        assertThat(cursor.keySet()).containsExactly("id", "ns", "firstBatch");
        assertThat(cursor.get("id")).isEqualTo(Long.valueOf(0));
        assertThat(cursor.get("ns")).isEqualTo(db.getName() + ".$cmd.listCollections");
        List<?> firstBatch = (List<?>) cursor.get("firstBatch");
        assertThat(firstBatch).isEmpty();
    }

    @Test
    void testListCollections() {
        List<String> collections = List.of("coll1", "coll2", "coll3");
        for (String collection : collections) {
            getCollection(collection).insertOne(json("_id: 1"));
        }

        Document result = db.runCommand(json("listCollections: 1"));
        assertThat(result.getDouble("ok")).isEqualTo(1.0);
        Document cursor = (Document) result.get("cursor");
        assertThat(cursor.keySet()).containsExactly("id", "ns", "firstBatch");
        assertThat(cursor.get("id")).isEqualTo(Long.valueOf(0));
        assertThat(cursor.get("ns")).isEqualTo(db.getName() + ".$cmd.listCollections");
        assertThat(cursor.get("firstBatch")).isInstanceOf(List.class);
        @SuppressWarnings("unchecked")
        List<Document> firstBatch = (List<Document>) cursor.get("firstBatch");

        assertThat(firstBatch).hasSameSizeAs(collections);

        Set<String> collectionNames = new HashSet<>();
        for (Document collection : firstBatch) {
            assertThat(collection).containsOnlyKeys("name", "options", "type", "idIndex", "info");
            String name = (String) collection.get("name");
            assertThat(collection.get("options")).isEqualTo(json(""));
            assertThat(collection.get("name")).isInstanceOf(String.class);
            assertThat(collection.get("type")).isEqualTo("collection");
            assertThat(collection.get("idIndex")).isEqualTo(json("key: {_id: 1}, name: '_id_', v: 2"));
            assertThat(collection.get("info")).isInstanceOf(Document.class);
            collectionNames.add(name);
        }

        assertThat(collectionNames).containsExactlyInAnyOrderElementsOf(collections);
    }

    @Test
    void testGetCollectionNames() {
        getCollection("foo").insertOne(json(""));
        getCollection("bar").insertOne(json(""));

        assertThat(db.listCollectionNames())
            .containsExactlyInAnyOrder("foo", "bar");
    }

    @Test
    public void testSystemNamespaces() {
        getCollection("foo").insertOne(json(""));
        getCollection("bar").insertOne(json(""));

        MongoCollection<Document> namespaces = db.getCollection("system.namespaces");
        assertThat(namespaces.find()).containsExactlyInAnyOrder(
            json("name: 'testdb.foo'"),
            json("name: 'testdb.bar'")
        );
    }

    @Test
    void testDatabaseStats() {
        Document stats = db.runCommand(new Document("dbStats", 1).append("scale", 1));
        assertThat(stats.getDouble("ok")).isEqualTo(1.0);
        assertThat(stats.getInteger("objects")).isZero();
        assertThat(stats.getInteger("collections")).isZero();
        assertThat(stats.getInteger("indexes")).isZero();
        assertThat(stats.getInteger("dataSize")).isZero();

        getCollection("foo").insertOne(json(""));
        getCollection("foo").insertOne(json(""));
        getCollection("bar").insertOne(json(""));

        stats = db.runCommand(new Document("dbStats", 1).append("scale", 1));
        assertThat(stats.getDouble("ok")).isEqualTo(1.0);
        assertThat(stats.getInteger("objects")).isEqualTo(3);
        assertThat(stats.getInteger("collections")).isEqualTo(2);
        assertThat(stats.getInteger("indexes")).isEqualTo(2);
        assertThat(stats.getDouble("dataSize")).isEqualTo(66.0);
    }

    @Test
    void testDeleteDecrementsCount() {
        collection.insertOne(json("key: 'value'"));
        assertThat(collection.countDocuments()).isEqualTo(1);
        collection.deleteOne(json(""));
        assertThat(collection.countDocuments()).isZero();
    }

    @Test
    void testDeleteInSystemNamespace() {
        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> getCollection("system.foobar").deleteOne(json("")))
            .withMessageStartingWith("Command execution failed on MongoDB server with error 73 (InvalidNamespace): 'Invalid system namespace: testdb.system.foobar'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> getCollection("system.namespaces").deleteOne(json("")))
            .withMessageStartingWith("Command execution failed on MongoDB server with error 73 (InvalidNamespace): 'Invalid system namespace: testdb.system.namespaces'");
    }

    @Test
    public void testUpdateInSystemNamespace() {
        for (String collectionName : List.of("system.foobar", "system.namespaces")) {
            MongoCollection<Document> collection = getCollection(collectionName);

            assertMongoWriteException(() -> collection.updateMany(eq("some", "value"), set("field", "value")),
                10156, "cannot update system collection");
        }
    }

    @Test
    void testDistinctQuery() {
        collection.insertOne(json("_id: 1, n: null"));
        collection.insertOne(json("_id: 2, n: 3"));
        collection.insertOne(json("_id: 3, n: 1"));
        collection.insertOne(json("_id: 4, n: 2"));
        collection.insertOne(json("_id: 5, n: 1.0"));
        collection.insertOne(json("_id: 6, n: 1"));
        collection.insertOne(json("_id: 7, n: -0.0"));
        collection.insertOne(json("_id: 8, n: 0"));

        assertThat(collection.distinct("n", Integer.class))
            .containsExactly(null, 0, 1, 2, 3);

        assertThat(collection.distinct("n", json("n: {$gt: 1}"), Integer.class))
            .containsExactly(2, 3);

        assertThat(collection.distinct("foobar", String.class)).isEmpty();

        assertThat(collection.distinct("_id", Integer.class))
            .hasSize((int) collection.countDocuments());
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/44
    @Test
    void testDistinctUuids_legacy() {
        MongoClientSettings legacyUuidSettings = MongoClientSettings.builder()
            .applyConnectionString(connectionString)
            .uuidRepresentation(UuidRepresentation.JAVA_LEGACY)
            .build();
        try (MongoClient clientWithLegacyUuid = MongoClients.create(legacyUuidSettings)) {
            MongoCollection<Document> collectionWithLegacyUuid = clientWithLegacyUuid.getDatabase(collection.getNamespace().getDatabaseName()).getCollection(collection.getNamespace().getCollectionName());

            collectionWithLegacyUuid.insertOne(json("_id: 1, n: null"));
            collectionWithLegacyUuid.insertOne(json("_id: 2").append("n", new UUID(0, 1)));
            collectionWithLegacyUuid.insertOne(json("_id: 3").append("n", new UUID(1, 0)));
            collectionWithLegacyUuid.insertOne(json("_id: 4").append("n", new UUID(0, 2)));
            collectionWithLegacyUuid.insertOne(json("_id: 5").append("n", new UUID(1, 1)));
            collectionWithLegacyUuid.insertOne(json("_id: 6").append("n", new UUID(1, 0)));

            assertThat(collectionWithLegacyUuid.distinct("n", UUID.class))
                .containsExactly(
                    null,
                    new UUID(0, 1),
                    new UUID(0, 2),
                    new UUID(1, 0),
                    new UUID(1, 1)
                );
        }
    }

    @Test
    void testDistinctUuids() {
        collection.insertOne(json("_id: 1, n: null"));
        collection.insertOne(json("_id: 2").append("n", new UUID(0, 1)));
        collection.insertOne(json("_id: 3").append("n", new UUID(1, 0)));
        collection.insertOne(json("_id: 4").append("n", new UUID(0, 2)));
        collection.insertOne(json("_id: 5").append("n", new UUID(1, 1)));
        collection.insertOne(json("_id: 6").append("n", new UUID(1, 0)));

        assertThat(collection.distinct("n", UUID.class))
            .containsExactly(
                null,
                new UUID(0, 1),
                new UUID(0, 2),
                new UUID(1, 0),
                new UUID(1, 1)
            );
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/70
    @Test
    void testDistinctArrayField() {
        collection.insertOne(json("_id: 1, n: null"));
        collection.insertOne(json("_id: 2").append("n", List.of(1, 2, 3)));
        collection.insertOne(json("_id: 3").append("n", List.of(3, 4, 5)));
        collection.insertOne(json("_id: 4").append("n", 6));

        assertThat(collection.distinct("n", Integer.class))
            .containsExactly(null, 1, 2, 3, 4, 5, 6);
    }

    @Test
    void testDistinct_documentArray() {
        collection.insertOne(json("_id: 1, n: null"));
        collection.insertOne(json("_id: 2, n: [{item: 1}, {item: 2}]"));
        collection.insertOne(json("_id: 3, n: {item: 3}"));
        collection.insertOne(json("_id: 4, n: {item: [4, 5]}"));
        collection.insertOne(json("_id: 5, n: {}"));

        assertThat(collection.distinct("n.item", Integer.class))
            .containsExactly(1, 2, 3, 4, 5);
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/178
    @Test
    void testDistinct_missingCollection() {
        MongoCollection<Document> missingCollection = db.getCollection("does-not-exist");

        assertThat(missingCollection.distinct("x", Integer.class)).isEmpty();
    }

    @Test
    public void testInsertQueryAndSortBinaryTypes() {
        byte[] highBytes = new byte[16];
        Arrays.fill(highBytes, (byte) 0xFF);

        collection.insertOne(json("_id: 1, n: null"));
        collection.insertOne(json("_id: 2").append("n", new UUID(0, 1)));
        collection.insertOne(json("_id: 3").append("n", new UUID(1, 0)));
        collection.insertOne(json("_id: 4, n: 'abc'"));
        collection.insertOne(json("_id: 5, n: 17"));
        collection.insertOne(json("_id: 6, n: [1, 2, 3]"));
        collection.insertOne(json("_id: 7").append("n", new byte[] { 0, 0, 0, 1 }));
        collection.insertOne(json("_id: 8").append("n", highBytes));
        collection.insertOne(json("_id: 9").append("n", new byte[0]));

        assertThat(collection.find(json("n: {$type: 5}")).sort(json("n: 1")))
            .containsExactly(
                json("_id: 9").append("n", new Binary(new byte[0])),
                json("_id: 7").append("n", new Binary(new byte[] { 0, 0, 0, 1 })),
                json("_id: 8").append("n", new Binary(highBytes)),
                json("_id: 2").append("n", new UUID(0, 1)),
                json("_id: 3").append("n", new UUID(1, 0))
            );

        assertThat(collection.find(new Document("n", new UUID(1, 0))))
            .containsExactly(
                json("_id: 3").append("n", new UUID(1, 0))
            );

        assertThat(collection.find(json("")).sort(json("n: 1")))
            .containsExactly(
                json("_id: 1, n: null"),
                json("_id: 6, n: [1, 2, 3]"),
                json("_id: 5, n: 17"),
                json("_id: 4, n: 'abc'"),
                json("_id: 9").append("n", new Binary(new byte[0])),
                json("_id: 7").append("n", new Binary(new byte[] { 0, 0, 0, 1 })),
                json("_id: 8").append("n", new Binary(highBytes)),
                json("_id: 2").append("n", new UUID(0, 1)),
                json("_id: 3").append("n", new UUID(1, 0))
            );
    }

    @Test
    public void testUuidAsId() {
        collection.insertOne(new Document("_id", new UUID(0, 1)));
        collection.insertOne(new Document("_id", new UUID(0, 2)));
        collection.insertOne(new Document("_id", new UUID(999999, 128)));

        assertMongoWriteException(() -> collection.insertOne(new Document("_id", new UUID(0, 1))),
            11000, "DuplicateKey",
            "E11000 duplicate key error collection: testdb.testcoll index: _id_ dup key: { _id: UUID(\"00000000-0000-0000-0000-000000000001\") }");

        assertMongoWriteException(() -> collection.insertOne(new Document("_id", new UUID(999999, 128))),
            11000, "DuplicateKey",
            "E11000 duplicate key error collection: testdb.testcoll index: _id_ dup key: { _id: UUID(\"00000000-000f-423f-0000-000000000080\") }");

        collection.deleteOne(new Document("_id", new UUID(0, 2)));

        assertThat(collection.find(json("")))
            .containsExactlyInAnyOrder(
                new Document("_id", new UUID(0, 1)),
                new Document("_id", new UUID(999999, 128))
            );
    }

    @Test
    public void testTypeMatching() {
        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 'abc'"));
        collection.insertOne(json("a: {b: {c: 123}}"));
        collection.insertOne(json("_id: {'$numberDecimal': '2'}"));

        assertThat(collection.find(json("_id: {$type: 2.0}")))
            .containsExactly(json("_id: 'abc'"));

        assertThat(collection.find(json("_id: {$type: [16, 'string']}")))
            .containsExactlyInAnyOrder(
                json("_id: 1"),
                json("_id: 'abc'")
            );

        assertThat(collection.find(json("_id: {$type: 'number'}")))
            .containsExactlyInAnyOrder(
                json("_id: 1"),
                json("_id: {'$numberDecimal': '2'}")
            );

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(json("n: {$type: []}")).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 9 (FailedToParse): 'n must match at least one type'");

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(json("'a.b.c': {$type: []}")).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 9 (FailedToParse): 'a.b.c must match at least one type'");

        assertThat(collection.find(json("a: {b: {$type: []}}"))).isEmpty();

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(json("n: {$type: 'abc'}")).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 2 (BadValue): 'Unknown type name alias: abc'");

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(json("n: {$type: null}")).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 14 (TypeMismatch): 'type must be represented as a number or a string'");

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(json("_id: {$type: 16.3}")).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 2 (BadValue): 'Invalid numerical type code: 16.3'");
    }

    @Test
    void testDistinctQueryWithDot() {
        collection.insertOne(json("a: {b: 1}"));
        collection.insertOne(json("a: {b: 1}"));
        collection.insertOne(json("a: {b: 1}"));
        collection.insertOne(json("a: {b: 2}"));
        collection.insertOne(json("a: {b: 3}"));
        collection.insertOne(json("a: {b: null}"));
        collection.insertOne(json("a: null"));

        assertThat(collection.distinct("a.b", Integer.class))
            .containsExactly(null, 1, 2, 3);

        assertThat(collection.distinct("a.c", Integer.class)).isEmpty();
    }

    @Test
    void testDropCollection() {
        collection.createIndex(new Document("n", 1));
        collection.createIndex(new Document("b", 1));

        collection.insertOne(json(""));
        assertThat(db.listCollectionNames()).contains(getCollectionName());

        assertThat(collection.listIndexes())
            .containsExactlyInAnyOrder(
                json("key: {_id: 1}").append("name", "_id_").append("v", 2),
                json("key: {n: 1}").append("name", "n_1").append("v", 2),
                json("key: {b: 1}").append("name", "b_1").append("v", 2)
            );

        collection.drop();
        assertThat(db.listCollectionNames()).doesNotContain(getCollectionName());

        assertThat(collection.listIndexes()).isEmpty();
    }

    @Test
    void testDropCollectionAlsoDropsFromDB() {
        collection.insertOne(json(""));
        collection.drop();
        assertThat(collection.countDocuments()).isZero();
        assertThat(db.listCollectionNames()).doesNotContain(getCollectionName());
    }

    @Test
    void testDropDatabaseAlsoDropsCollectionData() {
        collection.insertOne(json(""));
        db.drop();
        assertThat(collection.countDocuments()).isZero();
    }

    @Test
    void testDropDatabaseDropsAllData() {
        collection.insertOne(json("_id: 1"));
        MongoCollection<Document> collection2 = getCollection("testcoll2");
        collection2.insertOne(json("_id: 1"));

        db.drop();
        assertThat(listDatabaseNames()).doesNotContain(db.getName());
        assertThat(collection.countDocuments()).isZero();
        assertThat(db.listCollectionNames()).doesNotContain(getCollectionName(),
            collection2.getNamespace().getCollectionName());

        collection.insertOne(json("_id: 1"));
        collection2.insertOne(json("_id: 1"));
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/136
    @Test
    void testDropEmptyDatabase() {
        String emptyDatabaseName = "empty-db";
        MongoDatabase database = syncClient.getDatabase(emptyDatabaseName);
        database.drop();
        assertThat(syncClient.listDatabaseNames()).doesNotContain(emptyDatabaseName);
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/107
    @Test
    void testDropDatabaseAfterAddingIndexMultipleTimes() {
        collection.insertOne(json("_id: 1, a: 10"));
        for (int i = 0; i < 3; i++) {
            collection.createIndex(json("a: 1"), new IndexOptions().unique(true));
        }
        db.drop();
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/107
    @Test
    void testAddIndexAgainWithDifferentOptions() {
        collection.insertOne(json("_id: 1, a: 10"));
        collection.createIndex(json("a: 1"), new IndexOptions().unique(true));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.createIndex(json("a: 1"), new IndexOptions().unique(true).sparse(true)))
            .withMessageStartingWith("Command execution failed on MongoDB server with error 86 (IndexKeySpecsConflict): " +
                "'An existing index has the same name as the requested index. " +
                "When index names are not specified, they are auto generated and can cause conflicts. " +
                "Please refer to our documentation. " +
                "Requested index: { v: 2, unique: true, key: { a: 1 }, name: \"a_1\", sparse: true }, " +
                "existing index: { v: 2, unique: true, key: { a: 1 }, name: \"a_1\" }'");
    }

    @Test
    void testEmbeddedSort() {
        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));
        collection.insertOne(json("_id: 3"));
        collection.insertOne(json("_id: 4, counts: {done: 1}"));
        collection.insertOne(json("_id: 5, counts: {done: 2}"));

        assertThat(collection.find(ne("c", true)).sort(json("'counts.done': -1, _id: 1")))
            .containsExactly(
                json("_id: 5, counts: {done: 2}"),
                json("_id: 4, counts: {done: 1}"),
                json("_id: 1"),
                json("_id: 2"),
                json("_id: 3")
            );
    }

    @Test
    public void testEmbeddedSort_arrayOfDocuments() {
        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2, counts: {done: 1}"));
        collection.insertOne(json("_id: 3, counts: {done: 2}"));
        collection.insertOne(json("_id: 4, counts: [{done: 2}, {done: 1}]"));
        collection.insertOne(json("_id: 5, counts: [{done: 4}, {done: 2}]"));
        collection.insertOne(json("_id: 6, counts: {done: [3]}"));
        collection.insertOne(json("_id: 7, counts: {done: [1, 2]}"));
        collection.insertOne(json("_id: 8, counts: [1, 2]"));

        assertThat(collection.find(json("")).sort(json("\"counts.done\": -1, _id: 1")))
            .containsExactly(
                json("_id: 5, counts: [{done: 4}, {done: 2}]"),
                json("_id: 6, counts: {done: [3]}"),
                json("_id: 3, counts: {done: 2}"),
                json("_id: 4, counts: [{done: 2}, {done: 1}]"),
                json("_id: 7, counts: {done: [1, 2]}"),
                json("_id: 2, counts: {done: 1}"),
                json("_id: 1"),
                json("_id: 8, counts: [1, 2]")
            );
    }

    @Test
    void testFindAndModifyCommandEmpty() {
        Document cmd = new Document("findandmodify", getCollectionName());

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> db.runCommand(cmd))
            .withMessageStartingWith("Command execution failed on MongoDB server with error 9 (FailedToParse): 'Either an update or remove=true must be specified'");
    }

    @Test
    void testFindAndModifyCommandIllegalOp() {
        collection.insertOne(json("_id: 1"));

        Document cmd = new Document("findAndModify", getCollectionName());
        cmd.put("query", json("_id: 1"));
        cmd.put("update", new Document("$inc", json("_id: 1")));

        assertThat(collection.find().first()).isEqualTo(json("_id: 1"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> db.runCommand(cmd))
            .withMessageStartingWith("Command execution failed on MongoDB server with error 66 (ImmutableField): " +
                "'Plan executor error during findAndModify :: caused by :: " +
                "Performing an update on the path '_id' would modify the immutable field '_id'");
    }

    @Test
    void testFindAndModifyCommandUpdate() {
        collection.insertOne(json("_id: 1"));

        Document cmd = new Document("findAndModify", getCollectionName());
        cmd.put("query", json("_id: 1"));
        cmd.put("update", json("$inc: {a: 1}"));

        Document result = db.runCommand(cmd);
        assertThat(result.get("lastErrorObject")).isEqualTo(json("updatedExisting: true, n: 1"));
        assertThat(result.getDouble("ok")).isEqualTo(1.0);

        assertThat(collection.find().first()).isEqualTo(json("_id: 1, a: 1"));
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/75
    @Test
    void testFindAndModifyCommand_UpdateSameFields() {
        collection.insertOne(json("_id: 1"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(json("_id: 1"), json("$inc: {x: 0, a: 1}, $set: {a: 2}")))
            .withMessageStartingWith("Command execution failed on MongoDB server with error 40 (ConflictingUpdateOperators): 'Updating the path 'a' would create a conflict at 'a'");
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/75
    @Test
    void testFindAndModifyCommand_UpdateFieldAndItsSubfield() {
        collection.insertOne(json("_id: 1, a: {b: {c: 1}}"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(json("_id: 1"), json("$set: {'x': 1, 'a.b': {c: 1}}, $inc: {'a.b.c': 1}")))
            .withMessageStartingWith("Command execution failed on MongoDB server with error 40 (ConflictingUpdateOperators): 'Updating the path 'a.b.c' would create a conflict at 'a.b'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(json("_id: 1"), json("$set: {'x': 1, 'a.b.c': 1}, $unset: {'a.b': 1}")))
            .withMessageStartingWith("Command execution failed on MongoDB server with error 40 (ConflictingUpdateOperators): 'Updating the path 'a.b' would create a conflict at 'a.b'");
    }

    @Test
    void testFindOneAndUpdateError() {
        collection.insertOne(json("_id: 1, a: 1"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(json("_id: 1"), json("$inc: {_id: 1}")))
            .withMessageStartingWith("Command execution failed on MongoDB server with error 66 (ImmutableField): " +
                "'Plan executor error during findAndModify :: caused by ::" +
                " Performing an update on the path '_id' would modify the immutable field '_id'");
    }

    @Test
    void testFindOneAndUpdateFields() {
        collection.insertOne(json("_id: 1, a: 1"));
        Document result = collection.findOneAndUpdate(json("_id: 1"), json("$inc: {a: 1}"),
            new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));

        assertThat(result).isEqualTo(json("_id: 1, a: 2"));
    }

    @Test
    void testFineOneAndUpdateNotFound() {
        collection.insertOne(json("_id: 1, a: 1"));
        Document result = collection.findOneAndUpdate(json("_id: 2"), new Document("$inc", json("a: 1")));

        assertThat(result).isNull();
        assertThat(collection.countDocuments()).isEqualTo(1);
    }

    @Test
    void testFineOneAndUpdateRemove() {
        collection.insertOne(json("_id: 1, a: 1"));
        Document result = collection.findOneAndDelete(json("_id: 1"));

        assertThat(result).isEqualTo(json("_id: 1, a: 1"));
        assertThat(collection.countDocuments()).isZero();
    }

    // https://github.com/foursquare/fongo/issues/32
    @Test
    void testFineOneAndUpdateReturnNew() {
        collection.insertOne(json("_id: 1, a: 1, b: {c: 1}"));

        Document query = json("_id: 1");
        Document update = json("$inc: {a: 1, 'b.c': 1}");
        Document result = collection.findOneAndUpdate(query, update,
            new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));

        assertThat(result).isEqualTo(json("_id: 1, a: 2, b: {c: 2}"));
    }

    @Test
    void testFineOneAndUpdateMax() {
        collection.insertOne(json("_id: 1, a: 1, b: {c: 1}"));

        Document query = json("_id: 1");
        Document update = json("$max: {a: 2, 'b.c': 2, d : 'd'}");
        Document result = collection.findOneAndUpdate(query, update,
            new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));

        assertThat(result).isEqualTo(json("_id: 1, a: 2, b: {c: 2}, d : 'd'"));
    }

    @Test
    void testFineOneAndUpdateMin() {
        collection.insertOne(json("_id: 1, a: 2, b: {c: 1}"));

        Document query = json("_id: 1");
        Document update = json("$min: {a: 1, 'b.c': 2, d : 'd'}");
        Document result = collection.findOneAndUpdate(query, update,
            new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));

        assertThat(result).isEqualTo(json("_id: 1, a: 1, b: {c: 1}, d : 'd'"));
    }

    // https://github.com/foursquare/fongo/issues/32
    @Test
    void testFindOneAndUpdateReturnOld() {
        collection.insertOne(json("_id: 1, a: 1, b: {c: 1}"));

        Document query = json("_id: 1");
        Document update = json("$inc: {a: 1, 'b.c': 1}");
        Document result = collection.findOneAndUpdate(query, update,
            new FindOneAndUpdateOptions().returnDocument(ReturnDocument.BEFORE));

        assertThat(result).isEqualTo(json("_id: 1, a: 1, b: {c: 1}"));
        assertThat(collection.find(query).first()).isEqualTo(json("_id: 1, a: 2, b: {c: 2}"));
    }

    @Test
    void testFindOneAndUpdateSorted() {
        collection.insertOne(json("_id: 1, a: 15"));
        collection.insertOne(json("_id: 2, a: 10"));
        collection.insertOne(json("_id: 3, a: 20"));

        Document order = json("a: 1");
        Document result = collection.findOneAndUpdate(json(""), json("$inc: {a: 1}"),
            new FindOneAndUpdateOptions().sort(order).returnDocument(ReturnDocument.AFTER));
        assertThat(result).isEqualTo(json("_id: 2, a: 11"));

        order = json("a: -1");
        result = collection.findOneAndUpdate(json(""), json("$inc: {a: 1}"),
            new FindOneAndUpdateOptions().sort(order).returnDocument(ReturnDocument.AFTER));
        assertThat(result).isEqualTo(json("_id: 3, a: 21"));
    }

    @Test
    void testFindOneAndUpdateUpsert() {
        Document result = collection.findOneAndUpdate(json("_id: 1"), json("$inc: {a: 1}"),
            new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));

        assertThat(result).isEqualTo(json("_id: 1, a: 1"));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, a: 1"));
    }

    @Test
    void testFindOneAndUpdateUpsertReturnBefore() {
        Document result = collection.findOneAndUpdate(json("_id: 1"), json("$inc: {a: 1}"),
            new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.BEFORE));

        assertThat(result).isNull();
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, a: 1"));
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/60
    @Test
    void testFindOneAndUpdateWithArrayFilters() {
        collection.insertOne(json("_id: 1, grades: [95, 102, 90, 150]"));
        collection.insertOne(json("_id: 2, values: [[1, 2, 3], 'other']"));
        collection.insertOne(json("_id: 3, a: {b: [1, 2, 3]}"));

        collection.findOneAndUpdate(
            json("_id: 1"),
            json("$set: {'grades.$[element]': 'abc'}"),
            new FindOneAndUpdateOptions().arrayFilters(List.of(json("element: {$gte: 100}"))));

        assertThat(collection.find(json("_id: 1")).first()).isEqualTo(json("_id: 1, grades: [95, 'abc', 90, 'abc']"));

        collection.findOneAndUpdate(
            json("_id: 1"),
            json("$unset: {'grades.$[element]': 1}"),
            new FindOneAndUpdateOptions().arrayFilters(List.of(json("element: 'abc'"))));

        assertThat(collection.find(json("_id: 1")).first()).isEqualTo(json("_id: 1, grades: [95, null, 90, null]"));

        collection.findOneAndUpdate(
            json("_id: 1"),
            json("$inc: {'grades.$[element]': 1}"),
            new FindOneAndUpdateOptions().arrayFilters(List.of(json("element: 90"))));

        assertThat(collection.find(json("_id: 1")).first()).isEqualTo(json("_id: 1, grades: [95, null, 91, null]"));

        collection.findOneAndUpdate(
            json("_id: 2"),
            json("$pull: {'values.$[element]': 2}"),
            new FindOneAndUpdateOptions().arrayFilters(List.of(json("element: {$type: 'array'}"))));

        assertThat(collection.find(json("_id: 2")).first())
            .isEqualTo(json("_id: 2, values: [[1, 3], 'other']"));

        collection.findOneAndUpdate(
            json("_id: 3"),
            json("$mul: {'a.b.$[element]': 10}"),
            new FindOneAndUpdateOptions().arrayFilters(List.of(json("element: 2"))));

        assertThat(collection.find(json("_id: 3")).first())
            .isEqualTo(json("_id: 3, a: {b: [1, 20, 3]}"));
    }

    private static Stream<Arguments> findAndUpdate_upsert_idInQueryArguments() {
        return Stream.of(
            Arguments.of("_id: 'some value'", false, "_id: 'some value', value: 100"),
            Arguments.of("'$and': [{_id: 'some value'}]", false, "_id: 'some value', value: 100"),
            Arguments.of("'$and': [{_id: 'some value'}, {other: 123}]", false, "_id: 'some value', other: 123, value: 100"),
            Arguments.of("'$or': [{_id: 'some value'}]", false, "_id: 'some value', value: 100"),
            Arguments.of("'$or': [{_id: 'some value'}, {other: 123}]", true, "value: 100"),
            Arguments.of("'$or': [{_id: 'some value', other: 123}]", false, "_id: 'some value', other: 123, value: 100"),
            Arguments.of("'$or': [{_id: 'some value'}, {$and: [{other: 123}, {more: 'abc'}]}]", true, "value: 100"),
            Arguments.of("'$and': [{other: 123}, {$and: [{_id: 'some value'}, {more: 'abc'}]}]", false, "_id: 'some value', other: 123, more: 'abc', value: 100")
        );
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/208
    @ParameterizedTest
    @MethodSource("findAndUpdate_upsert_idInQueryArguments")
    void testFindAndUpdate_upsert_idInQuery(String query, boolean randomObjectIdExpected, String expectedDocument) {
        collection.findOneAndUpdate(
            json(query),
            json("{'$set': {value: 100}}"),
            new FindOneAndUpdateOptions().upsert(true));

        if (randomObjectIdExpected) {
            Document document = collection.find().first();
            assertThat(document.remove("_id")).isInstanceOf(ObjectId.class);
            assertThat(document).isEqualTo(json(expectedDocument));
        } else {
            assertThat(collection.find())
                .containsExactly(json(expectedDocument));
        }
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/60
    @Test
    void testUpdateManyWithArrayFilters() {
        collection.insertOne(json("_id: 1, values: [9, 102, 90, 150]"));
        collection.insertOne(json("_id: 2, values: [1, 2, 3, 50]"));

        collection.updateMany(
            json(""),
            json("$set: {'values.$[x]': 20}"),
            new UpdateOptions().arrayFilters(List.of(json("x: {$gt: 20}")))
        );

        assertThat(collection.find(json("")))
            .containsExactly(
                json("_id: 1, values: [9, 20, 20, 20]"),
                json("_id: 2, values: [1, 2, 3, 20]")
            );
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/60
    @Test
    void testUpdateOneWithArrayFilter() {
        collection.insertOne(json("_id: 1, values: [{name: 'A', active: false}, {name: 'B', active: false}]"));

        collection.updateOne(json("_id: 1"),
            json("$set: {'values.$[elem].active': true}"),
            new UpdateOptions().arrayFilters(List.of(json("'elem.name': {$in: ['A']}")))
        );

        assertThat(collection.find(json("")))
            .containsExactly(
                json("_id: 1, values: [{name: 'A', active: true}, {name: 'B', active: false}]")
            );
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/60
    @Test
    void testUpsertWithArrayFilters() {
        collection.updateOne(
            json("_id: 1, values: [0, 1]"),
            json("$set: {'values.$[x]': 20}"),
            new UpdateOptions()
                .upsert(true)
                .arrayFilters(List.of(json("x: 0")))
        );

        assertThat(collection.find(json("")))
            .containsExactly(
                json("_id: 1, values: [20, 1]")
            );
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/60
    @Test
    void testUpdateWithMultipleArrayFilters() {
        collection.insertOne(json("_id: 1, values: [9, 102, 90, 150]"));
        collection.insertOne(json("_id: 2, values: [1, 2, 30, 50]"));

        collection.updateMany(
            json(""),
            json("$set: {'values.$[tooLow]': 10, 'values.$[tooHigh]': 40}"),
            new UpdateOptions().arrayFilters(List.of(
                json("tooLow: {$lte: 10}"),
                json("tooHigh: {$gt: 40}")
            ))
        );

        assertThat(collection.find(json("")))
            .containsExactly(
                json("_id: 1, values: [10, 40, 40, 40]"),
                json("_id: 2, values: [10, 10, 30, 40]")
            );
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/86
    @Test
    void testUpdateWithMultipleComplexArrayFilters() {
        collection.insertOne(json("_id: 1, products: [" +
            "{id: 1, charges: [" +
            "{type: 'A', min: 0, max: 1}, " +
            "{type: 'A', min: 0, max: 2}, " +
            "{type: 'B', min: 0, max: 1}, " +
            "]}, " +
            "{id: 2, charges: [{type: 'A', min: 0, max: 1}, ]}, " +
            "]"));

        collection.updateMany(
            json(""),
            json("$set: {'products.$[product].charges.$[charge].amount': 10}"),
            new UpdateOptions().arrayFilters(List.of(
                json("'product.id': 1"),
                json("'charge.type': 'A', 'charge.min': 0, 'charge.max': 2")
            ))
        );

        assertThat(collection.find(json("")))
            .containsExactly(
                json("_id: 1, products: [" +
                    "{id: 1, charges: [" +
                    "{type: 'A', min: 0, max: 1}, " +
                    "{type: 'A', min: 0, max: 2, amount: 10}, " +
                    "{type: 'B', min: 0, max: 1}, " +
                    "]}, " +
                    "{id: 2, charges: [{type: 'A', min: 0, max: 1}, ]}, " +
                    "]")
            );
    }

    @Test
    void testFindOneAndUpdate_IllegalArrayFilters() {
        collection.insertOne(json("_id: 1, grades: 'abc', a: {b: 123}"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(
                json("_id: 1"),
                json("$set: {'grades': 'abc'}"),
                new FindOneAndUpdateOptions().arrayFilters(List.of(json("element: {$gte: 100}")))))
            .withMessageStartingWith("Command execution failed on MongoDB server with error 9 (FailedToParse): 'The array filter for identifier 'element' was not used in the update { $set: { grades: \"abc\" } }'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(
                json("_id: 1"),
                json("$set: {'grades': 'abc'}"),
                new FindOneAndUpdateOptions().arrayFilters(List.of(
                    json("element: {$gte: 100}"),
                    json("element: {$lt: 100}")
                ))))
            .withMessageStartingWith("Command execution failed on MongoDB server with error 9 (FailedToParse): 'Found multiple array filters with the same top-level field name element'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(
                json("_id: 1"),
                json("$set: {'grades.$[element]': 'abc'}"),
                new FindOneAndUpdateOptions().arrayFilters(List.of(json("a: {$gte: 100}, b: {$gte: 100}, c: {$gte: 10}")))))
            .withMessageStartingWith("Command execution failed on MongoDB server with error 9 (FailedToParse): 'Error parsing array filter :: caused by :: Expected a single top-level field name, found 'a' and 'b'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(
                json("_id: 1"),
                json("$set: {'grades.$[element]': 'abc'}"),
                new FindOneAndUpdateOptions().arrayFilters(List.of(json("")))))
            .withMessageStartingWith("Command execution failed on MongoDB server with error 9 (FailedToParse): 'Cannot use an expression without a top-level field name in arrayFilters'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(
                json("_id: 1"),
                json("$set: {'grades.$[element]': 'abc'}"),
                new FindOneAndUpdateOptions().arrayFilters(List.of(json("element: {$gte: 100}")))))
            .withMessageStartingWith("Command execution failed on MongoDB server with error 2 (BadValue): " +
                "'Plan executor error during findAndModify :: caused by :: " +
                "Cannot apply array updates to non-array element grades: \"abc\"'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(
                json("_id: 1"),
                json("$set: {'$[element]': 10}"),
                new FindOneAndUpdateOptions().arrayFilters(List.of(json("element: 2")))))
            .withMessageStartingWith("Command execution failed on MongoDB server with error 2 (BadValue): " +
                "'Cannot have array filter identifier (i.e. '$[<id>]') element in the first position in path '$[element]'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(
                json("_id: 1"),
                json("$set: {'grades.subGrades.$[element]': 'abc'}"),
                new FindOneAndUpdateOptions().arrayFilters(List.of(json("element: {$gte: 100}")))))
            .withMessageStartingWith("Command execution failed on MongoDB server with error 2 (BadValue): " +
                "'Plan executor error during findAndModify :: caused by :: " +
                "The path 'grades.subGrades' must exist in the document in order to apply array updates.'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(
                json("_id: 1"),
                json("$set: {'grades.$[some value]': 'abc'}"),
                new FindOneAndUpdateOptions().arrayFilters(List.of(json("'some value': {$gte: 100}")))))
            .withMessageStartingWith("Command execution failed on MongoDB server with error 2 (BadValue): " +
                "'Error parsing array filter :: caused by :: " +
                "The top-level field name must be an alphanumeric string beginning with a lowercase letter, found 'some value''");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(
                json("_id: 1"),
                json("$set: {'a.b.$[x]': 'abc'}"),
                new FindOneAndUpdateOptions().arrayFilters(List.of(json("x: {$gte: 100}")))))
            .withMessageStartingWith("Command execution failed on MongoDB server with error 2 (BadValue): " +
                "'Plan executor error during findAndModify :: caused by :: " +
                "Cannot apply array updates to non-array element b: 123'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(
                json("_id: 1"),
                json("$set: {'grades': 'abc'}"),
                new FindOneAndUpdateOptions().arrayFilters(List.of(json("'a.b': 10, b: 12")))))
            .withMessageStartingWith("Command execution failed on MongoDB server with error 9 (FailedToParse): " +
                "'Error parsing array filter :: caused by :: " +
                "Expected a single top-level field name, found 'a' and 'b''");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(
                json("_id: 1"),
                json("$set: {'grades': 'abc'}"),
                new FindOneAndUpdateOptions().arrayFilters(List.of(
                    json("'a.b': 10"),
                    json("'a.c': 10")
                ))))
            .withMessageStartingWith("Command execution failed on MongoDB server with error 9 (FailedToParse): " +
                "'Found multiple array filters with the same top-level field name a'");
    }

    @Test
    void testFindOneAndUpdate_IllegalArrayFiltersPaths() {
        collection.insertOne(json("_id: 1, grades: 'abc', a: {b: [1, 2, 3]}"));
        collection.insertOne(json("_id: 2, grades: 'abc', a: {b: [{c: 1}, {c: 2}, {c: 3}]}"));
        collection.insertOne(json("_id: 3, grades: 'abc', a: {b: [[[1, 2], [3, 4]]]}"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(
                json("_id: 1"),
                json("$set: {'a.b.$[x].c': 'abc'}"),
                new FindOneAndUpdateOptions().arrayFilters(List.of(json("x: {$gt: 1}")))))
            .withMessageStartingWith("Command execution failed on MongoDB server with error 28 (PathNotViable): " +
                "'Plan executor error during findAndModify :: caused by :: " +
                "Cannot create field 'c' in element {1: 2}");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(
                json("_id: 1"),
                json("$set: {'a.b.$[x].c.d': 'abc'}"),
                new FindOneAndUpdateOptions().arrayFilters(List.of(json("x: {$gt: 1}")))))
            .withMessageStartingWith("Command execution failed on MongoDB server with error 28 (PathNotViable): " +
                "'Plan executor error during findAndModify :: caused by :: " +
                "Cannot create field 'c' in element {1: 2}");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(
                json("_id: 2"),
                json("$set: {'a.b.$[].c.$[]': 'abc'}")))
            .withMessageStartingWith("Command execution failed on MongoDB server with error 2 (BadValue): " +
                "'Plan executor error during findAndModify :: caused by :: " +
                "Cannot apply array updates to non-array element c: 1");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(
                json("_id: 3"),
                json("$set: {'a.b.$[].0.c': 'abc'}")))
            .withMessageStartingWith("Command execution failed on MongoDB server with error 28 (PathNotViable): " +
                "'Plan executor error during findAndModify :: caused by :: " +
                "Cannot create field 'c' in element {0: [ 1, 2 ]}");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(
                json("_id: 3"),
                json("$set: {'a.b.$[].0.$[].c': 'abc'}")))
            .withMessageStartingWith("Command execution failed on MongoDB server with error 28 (PathNotViable): " +
                "'Plan executor error during findAndModify :: caused by :: " +
                "Cannot create field 'c' in element {0: 1}");
    }

    @Test
    void testFindAndRemoveFromEmbeddedList() {
        collection.insertOne(json("_id: 1, a: [1]"));
        Document result = collection.findOneAndDelete(json("_id: 1"));
        assertThat(result).isEqualTo(json("_id: 1, a: [1]"));
        assertThat(collection.countDocuments()).isZero();
    }

    @Test
    void testFindOne() {
        collection.insertOne(json("key: 'value'"));
        collection.insertOne(json("key: 'value'"));
        Document result = collection.find().first();
        assertThat(result).isNotNull();
        assertThat(result.get("_id")).isNotNull();
    }

    @Test
    void testFindOneById() {
        collection.insertOne(json("_id: 1"));
        Document result = collection.find(json("_id: 1")).first();
        assertThat(result).isEqualTo(json("_id: 1"));
        assertThat(collection.find(json("_id: 2")).first()).isNull();
    }

    @Test
    void testFindOneIn() {
        collection.insertOne(json("_id: 1"));
        Document result = collection.find(json("_id: {$in: [1, 2]}")).first();
        assertThat(result).isEqualTo(json("_id: 1"));
    }

    @Test
    void testFindWithLimit() {
        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));
        collection.insertOne(json("_id: 3"));
        collection.insertOne(json("_id: 4"));

        assertThat(collection.find().sort(json("_id: 1")).limit(2))
            .containsExactly(
                json("_id: 1"),
                json("_id: 2")
            );

        assertThat(collection.find().sort(json("_id: 1")).limit(-2))
            .containsExactly(
                json("_id: 1"),
                json("_id: 2")
            );
    }

    @Test
    void testFindInReverseNaturalOrder() {
        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));

        assertThat(collection.find().sort(json("$natural: -1")))
            .containsExactly(
                json("_id: 2"),
                json("_id: 1")
            );
    }

    @Test
    void testFindWithPattern() {
        collection.insertOne(json("_id: 'marta'"));
        collection.insertOne(json("_id: 'john', foo: 'bar'"));
        collection.insertOne(json("_id: 'jon', foo: 'ba'"));
        collection.insertOne(json("_id: 'jo'"));

        assertThat(collection.find(new Document("_id", Pattern.compile("mart"))))
            .containsExactly(json("_id: 'marta'"));

        assertThat(collection.find(new Document("foo", Pattern.compile("ba"))))
            .containsExactly(json("_id: 'john', foo: 'bar'"), json("_id: 'jon', foo: 'ba'"));

        assertThat(collection.find(new Document("foo", Pattern.compile("ba$"))))
            .containsExactly(json("_id: 'jon', foo: 'ba'"));
    }

    @Test
    void testFindWithQuery() {
        collection.insertOne(json("name: 'jon'"));
        collection.insertOne(json("name: 'leo'"));
        collection.insertOne(json("name: 'neil'"));
        collection.insertOne(json("name: 'neil'"));

        assertThat(collection.find(json("name: 'neil'"))).hasSize(2);
    }

    @Test
    void testFindWithSkipLimit() {
        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));
        collection.insertOne(json("_id: 3"));
        collection.insertOne(json("_id: 4"));

        assertThat(collection.find().sort(json("_id: 1")).limit(2).skip(2))
            .containsExactly(json("_id: 3"), json("_id: 4"));
    }

    @Test
    void testFindWithSkipLimitInReverseOrder() {
        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));
        collection.insertOne(json("_id: 3"));
        collection.insertOne(json("_id: 4"));

        assertThat(collection.find().sort(json("_id: -1")).limit(2).skip(2))
            .containsExactly(json("_id: 2"), json("_id: 1"));
    }

    @Test
    void testFindWithSkipLimitAfterDelete() {
        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));
        collection.insertOne(json("_id: 3"));
        collection.insertOne(json("_id: 4"));
        collection.insertOne(json("_id: 5"));

        collection.deleteOne(json("_id: 1"));
        collection.deleteOne(json("_id: 3"));

        assertThat(collection.find().sort(json("_id: 1")).limit(2).skip(2))
            .containsExactly(json("_id: 5"));
    }

    @Test
    void testFullUpdateWithSameId() {
        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2, b: 5"));
        collection.insertOne(json("_id: 3"));
        collection.insertOne(json("_id: 4"));

        collection.replaceOne(json("_id: 2, b: 5"), json("_id: 2, a: 5"));

        assertThat(collection.find(json("_id: 2")).first()).isEqualTo(json("_id: 2, a: 5"));
    }

    @Test
    void testGetCollection() {
        MongoCollection<Document> collection = getCollection("coll");
        getCollection("coll").insertOne(json(""));

        assertThat(collection).isNotNull();
        assertThat(db.listCollectionNames()).contains("coll");
    }

    @Test
    void testNullId() {
        collection.insertOne(json("_id: null, name: 'test'"));
        Document result = collection.find(json("name: 'test'")).first();
        assertThat(result).isNotNull();
        assertThat(result.getObjectId(Constants.ID_FIELD)).isNull();

        assertMongoWriteException(() -> collection.insertOne(json("_id: null")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: _id_ dup key: { _id: null }");

        assertThat(collection.countDocuments()).isEqualTo(1);
        assertThat(collection.find(json("_id: null")).first()).isEqualTo(json("_id: null, name: 'test'"));

        collection.deleteOne(json("_id: null"));
        assertThat(collection.countDocuments()).isZero();
    }

    @Test
    void testIdInQueryResultsInIndexOrder() {
        collection.insertOne(json("_id: 4"));
        collection.insertOne(json("_id: 3"));
        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));

        assertThat(collection.find(json("_id: {$in: [3, 2, 1]}")))
            .containsExactlyInAnyOrder(
                json("_id: 1"),
                json("_id: 2"),
                json("_id: 3")
            );

        // https://github.com/bwaldvogel/mongo-java-server/issues/57
        assertThat(collection.find(json("_id: {$in: [[1, 2, 3]]}")))
            .isEmpty();
    }

    @Test
    void testInQuery_Arrays() {
        collection.insertOne(json("_id: 1, v: [1, 2, 3]"));
        collection.insertOne(json("_id: 2, v: [1, 2]"));
        collection.insertOne(json("_id: 3, v: 50"));
        collection.insertOne(json("_id: 4, v: null"));
        collection.insertOne(json("_id: 5"));

        assertThat(collection.find(json("v: {$in: [[1, 2, 3], 50]}")))
            .containsExactly(
                json("_id: 1, v: [1, 2, 3]"),
                json("_id: 3, v: 50")
            );

        assertThat(collection.find(json("v: {$not: {$in: [[1, 2, 3], 50]}}")))
            .containsExactlyInAnyOrder(
                json("_id: 2, v: [1, 2]"),
                json("_id: 4, v: null"),
                json("_id: 5")
            );

        assertThat(collection.find(json("v: {$not: {$in: [2, 50]}}")))
            .containsExactlyInAnyOrder(
                json("_id: 4, v: null"),
                json("_id: 5")
            );

        assertThat(collection.find(json("v: {$not: {$in: [[1, 2], 50, null]}}")))
            .containsExactly(
                json("_id: 1, v: [1, 2, 3]")
            );
    }

    @Test
    void testIdNotAllowedToBeUpdated() {
        collection.insertOne(json("_id: 1"));

        assertMongoWriteException(() -> collection.replaceOne(json("_id: 1"), json("_id: 2, a: 4")),
            66, "ImmutableField", "After applying the update, the (immutable) field '_id' was found to have been altered to _id: 2");

        // test with $set

        assertMongoWriteException(() -> collection.updateOne(json("_id: 1"), new Document("$set", json("_id: 2"))),
            66, "ImmutableField", "Performing an update on the path '_id' would modify the immutable field '_id'");
    }

    @Test
    void testIllegalCommand() {
        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> db.runCommand(json("foo: 1")))
            .withMessageStartingWith("Command execution failed on MongoDB server with error 59 (CommandNotFound): 'no such command: 'foo'");
    }

    @Test
    public void testCommandThatTriggersAnInternalException() {
        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> db.runCommand(json("triggerInternalException: 1")))
            .withMessageStartingWith("Command execution failed on MongoDB server with error -1: 'Unknown error: For testing purposes'");
    }

    @Test
    void testInsert() {
        assertThat(collection.countDocuments()).isEqualTo(0);

        for (int i = 0; i < 3; i++) {
            collection.insertOne(new Document("_id", Integer.valueOf(i)));
        }

        assertThat(collection.countDocuments()).isEqualTo(3);

        collection.insertOne(json("foo: [1, 2, 3]"));

        collection.insertOne(new Document("foo", new byte[10]));
        Document insertedObject = new Document("foo", UUID.randomUUID());
        collection.insertOne(insertedObject);
        Document document = collection.find(insertedObject).first();
        assertThat(document).isEqualTo(insertedObject);
    }

    @Test
    void testInsertDuplicate() {
        assertThat(collection.countDocuments()).isEqualTo(0);

        collection.insertOne(json("_id: 1"));
        assertThat(collection.countDocuments()).isEqualTo(1);

        assertMongoWriteException(() -> collection.insertOne(json("_id: 1.0")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: _id_ dup key: { _id: 1.0 }");

        assertThat(collection.countDocuments()).isEqualTo(1);
    }

    @Test
    void testInsertDuplicateThrows() {
        collection.insertOne(json("_id: 1"));

        assertMongoWriteException(() -> collection.insertOne(json("_id: 1")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: _id_ dup key: { _id: 1 }");
    }

    @Test
    void testInsertDuplicateWithConcernThrows() {
        collection.insertOne(json("_id: 1"));

        assertMongoWriteException(() -> collection.withWriteConcern(WriteConcern.ACKNOWLEDGED).insertOne(json("_id: 1")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: _id_ dup key: { _id: 1 }");
    }

    @Test
    void testInsertIncrementsCount() {
        assertThat(collection.countDocuments()).isZero();
        collection.insertOne(json("key: 'value'"));
        assertThat(collection.countDocuments()).isEqualTo(1);
    }

    @Test
    void testInsertQuery() {
        assertThat(collection.countDocuments()).isEqualTo(0);

        Document insertedObject = json("_id: 1");
        insertedObject.put("foo", "bar");
        collection.insertOne(insertedObject);

        assertThat(collection.find(insertedObject).first()).isEqualTo(insertedObject);
        assertThat(collection.find(new Document("_id", Long.valueOf(1))).first()).isEqualTo(insertedObject);
        assertThat(collection.find(new Document("_id", Double.valueOf(1.0))).first()).isEqualTo(insertedObject);
        assertThat(collection.find(new Document("_id", Float.valueOf(1.0001f))).first()).isNull();
        assertThat(collection.find(json("foo: 'bar'")).first()).isEqualTo(insertedObject);
    }

    @Test
    void testInsertRemove() {
        for (int i = 0; i < 10; i++) {
            collection.insertOne(json("_id: 1"));
            assertThat(collection.countDocuments()).isEqualTo(1);
            collection.deleteOne(json("_id: 1"));
            assertThat(collection.countDocuments()).isZero();

            collection.insertOne(new Document("_id", i));
            collection.deleteOne(new Document("_id", i));
        }
        assertThat(collection.countDocuments()).isZero();
        collection.deleteOne(json("'doesnt exist': 1"));
        assertThat(collection.countDocuments()).isZero();
    }

    @Test
    public void testInsertInSystemNamespace() {
        assertMongoWriteException(() -> getCollection("system.foobar").insertOne(json("")),
            16459, "attempt to insert in system namespace");

        assertMongoWriteException(() -> getCollection("system.namespaces").insertOne(json("")),
            16459, "attempt to insert in system namespace");
    }

    @Test
    public void testListDatabaseNames() {
        assertThat(listDatabaseNames()).isEmpty();
        collection.insertOne(json(""));
        assertThat(listDatabaseNames()).containsExactly(db.getName());
        getDatabase().getCollection("some-collection").insertOne(json(""));
        assertThat(listDatabaseNames()).containsExactly("bar", db.getName());
    }

    private MongoDatabase getDatabase() {
        return syncClient.getDatabase(OTHER_TEST_DATABASE_NAME);
    }

    @Test
    void testQuery() {
        Document obj = collection.find(json("_id: 1")).first();
        assertThat(obj).isNull();
        assertThat(collection.countDocuments()).isEqualTo(0);
    }

    @Test
    void testQueryAll() {
        List<Document> inserted = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Document obj = new Document("_id", i);
            collection.insertOne(obj);
            inserted.add(obj);
        }
        assertThat(collection.countDocuments()).isEqualTo(10);

        assertThat(collection.find().sort(json("_id: 1")))
            .containsExactlyElementsOf(inserted);
    }

    @Test
    void testQueryCount() {
        for (int i = 0; i < 100; i++) {
            collection.insertOne(json(""));
        }
        assertThat(collection.countDocuments()).isEqualTo(100);

        Document obj = json("_id: 1");
        assertThat(collection.countDocuments(obj)).isEqualTo(0);
        collection.insertOne(obj);
        assertThat(collection.countDocuments(obj)).isEqualTo(1);
    }

    @Test
    void testQueryLimitEmptyQuery() {
        for (int i = 0; i < 5; i++) {
            collection.insertOne(json(""));
        }
        assertThat(collection.countDocuments(json(""), new CountOptions().limit(1))).isEqualTo(1);
        assertThat(collection.countDocuments(json(""), new CountOptions().limit(-1))).isEqualTo(5);
        assertThat(collection.countDocuments(json(""))).isEqualTo(5);
    }

    @Test
    void testQueryLimitSimpleQuery() {
        for (int i = 0; i < 5; i++) {
            collection.insertOne(json("a: 1"));
        }
        assertThat(collection.countDocuments(json("a: 1"), new CountOptions().limit(1))).isEqualTo(1);
        assertThat(collection.countDocuments(json("a: 1"), new CountOptions().limit(-1))).isEqualTo(5);
        assertThat(collection.countDocuments(json("a: 1"))).isEqualTo(5);
    }

    @Test
    void testQueryNull() {
        Document object = json("_id: 1");
        collection.insertOne(object);
        assertThat(collection.find(json("foo: null")).first()).isEqualTo(object);
    }

    @Test
    void testQuerySkipLimitEmptyQuery() {
        assertThat(collection.countDocuments(json(""), new CountOptions().skip(3))).isEqualTo(0);

        for (int i = 0; i < 10; i++) {
            collection.insertOne(json(""));
        }

        assertThat(collection.countDocuments(json(""), new CountOptions().skip(3))).isEqualTo(7);
        assertThat(collection.countDocuments(json(""), new CountOptions().skip(15))).isEqualTo(0);
        assertThat(collection.countDocuments(json(""), new CountOptions().skip(3).limit(5))).isEqualTo(5);
    }

    @Test
    void testQuerySkipLimitSimpleQuery() {
        assertThat(collection.countDocuments(json("a: 1"), new CountOptions().skip(3))).isEqualTo(0);

        for (int i = 0; i < 10; i++) {
            collection.insertOne(json("a: 1"));
        }

        assertThat(collection.countDocuments(json("a: 1"), new CountOptions().skip(3))).isEqualTo(7);
        assertThat(collection.countDocuments(json("a: 1"), new CountOptions().skip(3).limit(5))).isEqualTo(5);
        assertThat(collection.countDocuments(json("a: 1"), new CountOptions().skip(15).limit(5))).isEqualTo(0);
    }

    @Test
    void testQuerySort() {
        Random random = new Random(4711);
        for (int i = 0; i < 10; i++) {
            collection.insertOne(new Document("_id", Double.valueOf(random.nextDouble())));
        }

        assertThat(collection.find().sort(json("_id: 1")))
            .extracting(d -> d.getDouble("_id"))
            .isSorted();

        assertThat(collection.find().sort(json("_id: -1")))
            .extracting(d -> d.getDouble("_id"))
            .isSortedAccordingTo(Comparator.comparingDouble(Double::doubleValue).reversed());
    }

    @Test
    void testQueryWithFieldSelector() {
        collection.insertOne(json("foo: 'bar'"));
        collection.insertOne(json("foo: null"));

        Document obj = collection.find(json("")).projection(json("foo: 1")).first();
        assertThat(obj).containsOnlyKeys("_id", "foo");

        obj = collection.find(json("foo: 'bar'")).projection(json("_id: 1")).first();
        assertThat(obj).containsOnlyKeys("_id");

        obj = collection.find(json("foo: null")).projection(json("_id: 0, foo: 1")).first();
        assertThat(obj).isEqualTo(json("foo: null"));

        obj = collection.find(json("foo: 'bar'")).projection(json("_id: 0, foo: 1")).first();
        assertThat(obj).containsOnlyKeys("foo");
    }

    @Test
    void testQueryWithDotNotationFieldSelector() {
        collection.insertOne(json("_id: 1, index: false, foo: {a: 'a1', b: 0}"));
        collection.insertOne(json("_id: 2, foo: {a: null, b: null}"));
        Document obj = collection.find(json("_id: 1")).projection(json("'foo.a': 1, 'foo.b': 1")).first();
        assertThat(obj).isEqualTo(json("_id: 1, foo: {a: 'a1', b: 0}"));

        obj = collection.find(json("_id: 1")).projection(json("'foo.a': 1")).first();
        assertThat(obj).isEqualTo(json("_id: 1, foo: {a: 'a1'}"));

        obj = collection.find(json("_id: 1")).projection(json("'foo.a': 1, index: 1, _id: 0")).first();
        assertThat(obj).isEqualTo(json("foo: {a: 'a1'}, index: false"));

        obj = collection.find(json("_id: 1")).projection(json("foo: 1, _id: 0")).first();
        assertThat(obj).isEqualTo(json("foo: {a: 'a1', b: 0}"));

        obj = collection.find(json("_id: 1")).projection(json("'foo.a.b.c.d': 1")).first();
        assertThat(obj).isEqualTo(json("_id: 1, foo: {}"));

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(json("_id: 1")).projection(json("'foo..': 1")).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 40353 (Location40353): 'FieldPath must not end with a '.'.'");

        obj = collection.find(json("_id: 2")).projection(json("'foo.a.b': 1, 'foo.b': 1, 'foo.c.d': 1")).first();
        assertThat(obj).isEqualTo(json("_id: 2, foo: {b: null}"));

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(json("_id: 2")).projection(json("'foo.a.b': 1, 'foo.b': 1, 'foo.c': 1, 'foo.c.d': 1")).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 31249 (Location31249): 'Path collision at foo.c.d remaining portion c.d'");

        obj = collection.find(json("_id: 2")).projection(json("'foo.a': 1")).first();
        assertThat(obj).isEqualTo(json("_id: 2, foo: {a: null}"));

        obj = collection.find(json("_id: 2")).projection(json("'foo.c': 1")).first();
        assertThat(obj).isEqualTo(json("_id: 2, foo: {}"));
    }

    @Test
    void testQueryWithDotNotationFieldSelector_Array() {
        collection.insertOne(json("_id: 1, values: [1, 2, {x: 100, y: 10}, {x: 200}]"));

        Document obj = collection.find(json("_id: 1")).projection(json("'values.0': 1, 'values.x': 1")).first();
        assertThat(obj).isEqualTo(json("_id: 1, values: [{x: 100}, {x: 200}]"));

        obj = collection.find(json("_id: 1")).projection(json("'values.y': 1")).first();
        assertThat(obj).isEqualTo(json("_id: 1, values: [{y: 10}, {}]"));

        obj = collection.find(json("_id: 1")).projection(json("'values.x': 1, 'values.y': 1")).first();
        assertThat(obj).isEqualTo(json("_id: 1, values: [{x: 100, y: 10}, {x: 200}]"));
    }

    @Test
    void testQueryWithDocumentAsFieldSelection() {
        collection.insertOne(json("_id: 1"));

        assertThat(collection.find(json("_id: 1")).projection(json("values: {x: 1}")).first())
            .isEqualTo(json("_id: 1"));

        assertThat(collection.find(json("_id: 1")).projection(json("values: {x: 1, y: 1}")).first())
            .isEqualTo(json("_id: 1"));
    }

    @Test
    public void testQuerySystemNamespace() {
        assertThat(getCollection("system.foobar").find().first()).isNull();
        assertThat(db.listCollectionNames()).isEmpty();

        collection.insertOne(json(""));
        Document expectedObj = new Document("name", collection.getNamespace().getFullName());
        Document coll = getCollection("system.namespaces").find(expectedObj).first();
        assertThat(coll).isEqualTo(expectedObj);
    }

    @Test
    void testQueryAllExpression() {
        collection.insertOne(json("a: [{x: 1}, {x: 2}]"));
        collection.insertOne(json("a: [{x: 2}, {x: 3}]"));

        assertThat(collection.countDocuments(json("'a.x': {$all: [1, 2]}"))).isEqualTo(1);
        assertThat(collection.countDocuments(json("'a.x': {$all: [2, 3]}"))).isEqualTo(1);
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/36
    @Test
    void testAndQueryWithAllAndNin() {
        collection.insertOne(json("_id: 1, tags: ['A', 'B']"));
        collection.insertOne(json("_id: 2, tags: ['A', 'D']"));
        collection.insertOne(json("_id: 3, tags: ['A', 'C']"));
        collection.insertOne(json("_id: 4, tags: ['C', 'D']"));

        assertThat(collection.find(json("$and: [{'tags': {$all: ['A']}}, {'tags': {$nin: ['B', 'C']}}]")))
            .containsExactly(json("_id: 2, tags: ['A', 'D']"));
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/215
    @Test
    void testMatchesNinFieldInArray() {
        collection.insertOne(json("_id: 1, tags: [{'value': 'A'}, {'value': 'D'}]"));
        collection.insertOne(json("_id: 2, tags: [{'value': 'A'}, {'value': 'B'}]"));
        collection.insertOne(json("_id: 3, tags: [{'value': 'A'}, {'value': 'C'}]"));

        assertThat(collection.find(json("'tags.value': {$nin: ['B', 'C']}")))
            .containsExactly(json("_id: 1, tags: [{'value': 'A'}, {'value': 'D'}]"));
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/220
    @Test
    void testMatchesNeFieldInArray() {
        collection.insertOne(json("_id: 1, tags: [{'value': 'A'}, {'value': 'D'}]"));
        collection.insertOne(json("_id: 2, tags: [{'value': 'A'}, {'value': 'B'}]"));
        collection.insertOne(json("_id: 3, tags: [{'value': 'A'}, {'value': 'C'}]"));

        assertThat(collection.find(json("'tags.value': {$ne: 'B'}")))
            .containsExactly(
            	json("_id: 1, tags: [{'value': 'A'}, {'value': 'D'}]"),
            	json("_id: 3, tags: [{'value': 'A'}, {'value': 'C'}]"));
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/7
    @Test
    void testMatchesNotIn() {
        Document document1 = json("_id: 1, code: 'c1', map: {key1: 'value 1.1', key2: ['value 2.1']}");
        Document document2 = json("_id: 2, code: 'c1', map: {key1: 'value 1.2', key2: ['value 2.2']}");
        Document document3 = json("_id: 3, code: 'c1', map: {key1: 'value 2.1', key2: ['value 2.1']}");
        Document document4 = json("_id: 4, values: [1, 2, 3]");
        Document document5 = json("_id: 5, values: null");

        collection.insertMany(List.of(document1, document2, document3, document4, document5));

        assertThat(collection.find(json("'map.key2': {$nin: ['value 2.2']}")))
            .containsExactly(document1, document3, document4, document5);

        assertThat(collection.find(json("'map.key2': {$not: {$in: ['value 2.2']}}")))
            .containsExactly(document1, document3, document4, document5);

        assertThat(collection.find(json("'map.key2': {$not: {$nin: ['value 2.2']}}")))
            .containsExactly(document2);

        assertThat(collection.find(json("'map.key2': {$not: {$not: {$in: ['value 2.2']}}}")))
            .containsExactly(document2);

        assertThat(collection.find(json("values: {$nin: []}")))
            .containsExactly(document1, document2, document3, document4, document5);

        assertThat(collection.find(json("values: {$nin: [1]}")))
            .containsExactly(document1, document2, document3, document5);

        assertThat(collection.find(json("values: {$nin: [1, 2]}")))
            .containsExactly(document1, document2, document3, document5);

        assertThat(collection.find(json("values: {$nin: [null]}")))
            .containsExactly(document4);
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/96
    @Test
    void testAndQueryWithAllAndSize() {
        collection.insertOne(json("_id: 1, list: ['A', 'B']"));
        collection.insertOne(json("_id: 2, list: ['A', 'B', 'C']"));

        assertThat(collection.find(json("$and: [{list: {$size: 2}}, {list: {$all: ['A', 'B']}}]}")))
            .containsExactly(json("_id: 1, list: ['A', 'B']"));

        assertThat(collection.find(json("list: {$all: ['A', 'B'], $size: 2}")))
            .containsExactly(json("_id: 1, list: ['A', 'B']"));
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/36
    @Test
    void testMatchesAllWithEmptyCollection() {
        collection.insertOne(json("_id: 1, text: 'TextA', tags: []"));
        collection.insertOne(json("_id: 2, text: 'TextB', tags: []"));
        collection.insertOne(json("_id: 3, text: 'TextA', tags: ['A']"));

        assertThat(collection.find(json("$and: [{'text': 'TextA'}, {'tags': {$all: []}}]"))).isEmpty();
    }

    @Test
    public void testQueryWithSubdocumentIndex() {
        collection.createIndex(json("action: {actionId: 1}"), new IndexOptions().unique(true));

        collection.insertOne(json("action: {actionId: 1}, value: 'a'"));
        collection.insertOne(json("action: {actionId: 2}, value: 'b'"));
        collection.insertOne(json("action: {actionId: 3}, value: 'c'"));

        Document foundWithNestedDocument = collection.find(json("action: {actionId: 2}")).first();
        assertThat(foundWithNestedDocument.get("value")).isEqualTo("b");

        Document foundWithDotNotation = collection.find(json("'action.actionId': 2")).first();
        assertThat(foundWithDotNotation.get("value")).isEqualTo("b");
    }

    // see https://github.com/bwaldvogel/mongo-java-server/issues/12
    @Test
    void testQueryBinaryData() {
        byte[] firstBytes = new byte[] { 0x01, 0x02, 0x03 };
        byte[] secondBytes = new byte[] { 0x03, 0x02, 0x01 };

        collection.insertOne(new Document("_id", 1).append("test", firstBytes));
        collection.insertOne(new Document("_id", 2).append("test", secondBytes));

        Document first = collection.find(new Document("test", firstBytes)).first();
        assertThat(first).isNotNull();
        assertThat(first.get("_id")).isEqualTo(1);

        Document second = collection.find(new Document("test", secondBytes)).first();
        assertThat(second).isNotNull();
        assertThat(second.get("_id")).isEqualTo(2);
    }

    @Test
    void testRemove() {
        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));
        collection.insertOne(json("_id: 3"));
        collection.insertOne(json("_id: 4"));

        collection.deleteOne(json("_id: 2"));

        assertThat(collection.find(json("_id: 2")).first()).isNull();
        assertThat(collection.countDocuments()).isEqualTo(3);

        collection.deleteMany(json("_id: {$gte: 3}"));
        assertThat(collection.countDocuments()).isEqualTo(1);
        assertThat(collection.find().first()).isEqualTo(json("_id: 1"));
    }

    @Test
    void testRemoveSingle() {
        Document obj = new Document("_id", ObjectId.get());
        collection.insertOne(obj);
        collection.deleteOne(obj);
    }

    @Test
    void testRemoveReturnsModifiedDocumentCount() {
        collection.insertOne(json(""));
        collection.insertOne(json(""));

        DeleteResult result = collection.deleteMany(json(""));
        assertThat(result.getDeletedCount()).isEqualTo(2);

        result = collection.deleteMany(json(""));
        assertThat(result.getDeletedCount()).isEqualTo(0);
    }

    @Test
    public void testReservedCollectionNames() {
        assertMongoWriteException(() -> getCollection("foo$bar").insertOne(json("")),
            10093, "cannot insert into reserved $ collection");

        String veryLongString = repeat("verylongstring", 5);
        assertMongoWriteException(() -> getCollection(veryLongString).insertOne(json("")),
            10080, "ns name too long, max size is 128");
    }

    private static String repeat(String str, int num) {
        String repeated = str;
        for (int i = 0; i < num; i++) {
            repeated += repeated;
        }
        return repeated;
    }

    @Test
    public void testServerStatus() {
        verifyServerStatus(runCommand("serverStatus"));
        verifyServerStatus(getDatabase().runCommand(json("serverStatus:1")));
    }

    private void verifyServerStatus(Document serverStatus) {
        assertThat(serverStatus.getDouble("ok")).isEqualTo(1);
        assertThat(serverStatus.get("uptime")).isInstanceOf(Number.class);
        assertThat(serverStatus.get("uptimeMillis")).isInstanceOf(Long.class);
        Instant serverTime = ((Date) serverStatus.get("localTime")).toInstant();
        assertThat(serverTime).isEqualTo(clock.instant());

        Document connections = (Document) serverStatus.get("connections");
        assertThat(connections.get("current")).isNotNull();
    }

    @Test
    void testServerStatusWithOpenCursors() {
        for (int i = 0; i < 20; i++) {
            collection.insertOne(new Document("_id", i + 1));
        }

        try (MongoCursor<Document> cursor1 = collection.find().batchSize(10).cursor();
             MongoCursor<Document> cursor2 = collection.find().batchSize(10).cursor()) {
            log.debug("Opened {} and {}", cursor1.getServerCursor(), cursor2.getServerCursor());
            Document serverStatus = runCommand("serverStatus");
            assertThat(serverStatus.getDouble("ok")).isEqualTo(1);

            Document metrics = serverStatus.get("metrics", Document.class);
            Document cursorMetrics = metrics.get("cursor", Document.class);
            assertThat(cursorMetrics.getLong("timedOut")).isZero();
            Document openCursors = cursorMetrics.get("open", Document.class);
            assertThat(openCursors.getLong("noTimeout")).isZero();
            assertThat(openCursors.getLong("pinned")).isZero();
            assertThat(openCursors.getLong("total")).isEqualTo(2);
        }
    }

    @Test
    void testPing() {
        assertThat(runCommand("ping").getDouble("ok")).isEqualTo(1.0);
        assertThat(runCommand(json("ping: true")).getDouble("ok")).isEqualTo(1.0);
        assertThat(runCommand(json("ping: 2.0")).getDouble("ok")).isEqualTo(1.0);
        assertThat(getDatabase().runCommand(json("ping: true")).getDouble("ok")).isEqualTo(1.0);
    }

    @Test
    void testReplSetGetStatus() {
        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> runCommand("replSetGetStatus"))
            .withMessageStartingWith("Command execution failed on MongoDB server with error 76 (NoReplicationEnabled): 'not running with --replSet'");
    }

    @Test
    void testWhatsMyUri() {
        for (String dbName : new String[] { ADMIN_DB_NAME, "local", "test" }) {
            Document result = syncClient.getDatabase(dbName).runCommand(new Document("whatsmyuri", 1));
            assertThat(result.get("you")).isNotNull();
            assertThat(result.get("you").toString()).matches("\\d+\\.\\d+\\.\\d+\\.\\d+:\\d*");
        }
    }

    @Test
    public void testSortDocuments() {
        collection.insertOne(json("_id: 1, a: {b: 1}"));
        collection.insertOne(json("_id: 2, a: {b: 2}"));
        collection.insertOne(json("_id: 3, a: 3"));
        collection.insertOne(json("_id: 4, a: {c: 1}"));

        assertThat(collection.find().sort(json("a: 1")))
            .containsExactly(
                json("_id: 3, a: 3"),
                json("_id: 1, a: {b: 1}"),
                json("_id: 2, a: {b: 2}"),
                json("_id: 4, a: {c: 1}")
            );

        assertThat(collection.find().sort(json("a: -1")))
            .containsExactly(
                json("_id: 4, a: {c: 1}"),
                json("_id: 2, a: {b: 2}"),
                json("_id: 1, a: {b: 1}"),
                json("_id: 3, a: 3")
            );
    }

    @Test
    public void testSort() {
        collection.insertOne(json("_id: 1, a: null"));
        collection.insertOne(json("_id: 2, a: 1"));
        collection.insertOne(json("_id: 3, a: 2"));
        collection.insertOne(json("_id: 4"));
        collection.insertOne(json("_id: 5, a: 3"));
        collection.insertOne(json("_id: 6, a: 4"));
        collection.insertOne(json("_id: 7, a: 'abc'"));
        collection.insertOne(json("_id: 8, a: 'zzz'"));
        collection.insertOne(json("_id: 9, a: 1.0"));

        assertThat(collection.find().sort(json("a: 1, _id: 1")))
            .containsExactly(
                json("_id: 1, a: null"),
                json("_id: 4"),
                json("_id: 2, a: 1"),
                json("_id: 9, a: 1.0"),
                json("_id: 3, a: 2"),
                json("_id: 5, a: 3"),
                json("_id: 6, a: 4"),
                json("_id: 7, a: 'abc'"),
                json("_id: 8, a: 'zzz'")
            );

        assertThat(collection.find().sort(json("a: -1, _id: 1")))
            .containsExactly(
                json("_id: 8, a: 'zzz'"),
                json("_id: 7, a: 'abc'"),
                json("_id: 6, a: 4"),
                json("_id: 5, a: 3"),
                json("_id: 3, a: 2"),
                json("_id: 2, a: 1"),
                json("_id: 9, a: 1.0"),
                json("_id: 1, a: null"),
                json("_id: 4")
            );
    }

    @Test
    void testSortByEmbeddedKey() {
        collection.insertOne(json("_id: 1, a: {b: 1}"));
        collection.insertOne(json("_id: 2, a: {b: 2}"));
        collection.insertOne(json("_id: 3, a: {b: 3}"));
        assertThat(collection.find().sort(json("'a.b': -1")))
            .containsExactly(
                json("_id: 3, a: {b: 3}"),
                json("_id: 2, a: {b: 2}"),
                json("_id: 1, a: {b: 1}")
            );
    }

    @Test
    void testUpdate() {
        Document object = json("_id: 1");
        Document newObject = json("_id: 1, foo: 'bar'");

        collection.insertOne(object);
        UpdateResult result = collection.replaceOne(object, newObject);
        assertThat(result.getModifiedCount()).isEqualTo(1);
        assertThat(result.getUpsertedId()).isNull();
        assertThat(collection.find(object).first()).isEqualTo(newObject);
    }

    @Test
    void testUpdateNothing() {
        Document object = json("_id: 1");
        UpdateResult result = collection.replaceOne(object, object);
        assertThat(result.getModifiedCount()).isEqualTo(0);
        assertThat(result.getMatchedCount()).isEqualTo(0);
        assertThat(result.getUpsertedId()).isNull();
    }

    @Test
    void testUpdateBlank() {
        Document document = json("'': 1, _id: 2, a: 3, b: 4");
        collection.insertOne(document);

        collection.updateOne(json(""), json("$set: {c: 5}"));
        assertThat(collection.find().first()).isEqualTo(json("'': 1, _id: 2, a: 3, b: 4, c: 5"));
    }

    @Test
    void testUpdateEmptyPositional() {
        collection.insertOne(json(""));
        assertMongoWriteException(() -> collection.updateOne(json(""), json("$set: {'a.$.b': 1}")),
            2, "BadValue", "The positional operator did not find the match needed from the query.");
    }

    @Test
    void testUpdateMultiplePositional() {
        collection.insertOne(json("a: {b: {c: 1}}"));
        assertMongoWriteException(() -> collection.updateOne(json("'a.b.c': 1"), json("$set: {'a.$.b.$.c': 1}")),
            2, "BadValue", "Too many positional (i.e. '$') elements found in path 'a.$.b.$.c'");
    }

    @Test
    void testUpdateIllegalFieldName() {
        // Disallow $ in field names - SERVER-3730

        collection.insertOne(json("x: 1"));

        collection.updateOne(json("x: 1"), json("$set: {y: 1}")); // ok

        assertMongoWriteException(() -> collection.updateOne(json("x: 1"), json("$set: {$z: 1}")),
            52, "DollarPrefixedFieldName", "The dollar ($) prefixed field '$z' in '$z' is not allowed in the context of an update's replacement document. " +
                "Consider using an aggregation pipeline with $replaceWith.");

        // unset ok to remove bad fields
        collection.updateOne(json("x: 1"), json("$unset: {$z: 1}"));

        assertMongoWriteException(() -> collection.updateOne(json("x: 1"), json("$inc: {$z: 1}")),
            52, "DollarPrefixedFieldName", "The dollar ($) prefixed field '$z' in '$z' is not allowed in the context of an update's replacement document. " +
                "Consider using an aggregation pipeline with $replaceWith.");

        assertMongoWriteException(() -> collection.updateOne(json("x: 1"), json("$push: {$z: [1, 2, 3]}")),
            52, "DollarPrefixedFieldName", "The dollar ($) prefixed field '$z' in '$z' is not allowed in the context of an update's replacement document. " +
                "Consider using an aggregation pipeline with $replaceWith.");
    }

    @Test
    void testUpdateSubdocument() {
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> collection.updateOne(json(""), json("'a.b.c': 123")))
            .withMessage("All update operators must start with '$', but 'a.b.c' does not");
    }

    @ParameterizedTest
    @ValueSource(strings = { "a.", "a..", "a.b", "a.b.", "a.....111" })
    void testInsertWithSpecialFieldNames(String specialFieldName) {
        Document document = json("_id: 1").append(specialFieldName, 1);

        collection.insertOne(document);

        assertThat(collection.find().first()).isEqualTo(document);
    }

    @Test
    void testUpdateIdNoChange() {
        collection.insertOne(json("_id: 1"));
        collection.replaceOne(json("_id: 1"), json("_id: 1, a: 5"));

        assertThat(collection.find(json("_id: 1")).first()).isEqualTo(json("_id: 1, a: 5"));

        collection.updateOne(json("_id: 1"), json("$set: {_id: 1, b: 3}"));

        assertThat(collection.find(json("_id: 1")).first()).isEqualTo(json("_id: 1, a: 5, b: 3"));

        // test with $set

        collection.updateOne(json("_id: 1"), json("$set: {_id: 1, a: 7}"));

        assertThat(collection.find(json("_id: 1")).first()).isEqualTo(json("_id: 1, a: 7, b: 3"));
    }

    @Test
    void testUpdatePush() {
        Document idObj = json("_id: 1");
        collection.insertOne(idObj);
        collection.updateOne(idObj, json("$push: {'field.subfield.subsubfield': 'value'}"));
        Document expected = json("_id: 1, field: {subfield: {subsubfield: ['value']}}");
        assertThat(collection.find(idObj).first()).isEqualTo(expected);

        // push to non-array
        collection.updateOne(idObj, json("$set: {field: 'value'}"));
        assertMongoWriteException(() -> collection.updateOne(idObj, json("$push: {field: 'value'}")),
            2, "BadValue", "The field 'field' must be an array but is of type string in document {_id: 1}");

        // push with multiple fields

        Document pushObj = json("$push: {field1: 'value', field2: 'value2'}");
        collection.updateOne(idObj, pushObj);

        expected = json("_id: 1, field: 'value', field1: ['value'], field2: ['value2']");
        assertThat(collection.find(idObj).first()).isEqualTo(expected);

        // push duplicate
        pushObj = json("$push: {field1: 'value'}");
        collection.updateOne(idObj, pushObj);
        expected.put("field1", List.of("value", "value"));
        assertThat(collection.find(idObj).first()).isEqualTo(expected);
    }

    @Test
    void testPushDollarFieldName() {
        collection.insertOne(json("_id: 1"));

        collection.updateOne(json(""), json("$push: {value: {$position: 1}}"));

        assertThat(collection.find())
            .containsExactly(json("_id: 1, value: [{$position: 1}]"));
    }

    @Test
    void testUpdatePushEach() {
        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2, value: [0]"));

        collection.updateMany(json(""), json("$push: {value: {$each: [1, 2, 3, {key: 'value'}]}}"));

        assertThat(collection.find())
            .containsExactlyInAnyOrder(
                json("_id: 1, value: [1, 2, 3, {key: 'value'}]"),
                json("_id: 2, value: [0, 1, 2, 3, {key: 'value'}]")
            );
    }

    @Test
    public void testUpdatePushSlice() {
        collection.insertOne(json("_id: 1"));

        collection.updateOne(json(""), json("$push: {value: {$each: ['a', 'b', 'c'], $slice: 4}}"));
        collection.updateOne(json(""), json("$push: {value: {$each: [1, 2, 3], $slice: 4}}"));

        assertThat(collection.find())
            .containsExactly(json("_id: 1, value: ['a', 'b', 'c', 1]"));
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/166
    @Test
    void testUpdatePushWithNegativeAndZeroSlice() {
        collection.insertOne(json("_id: 1"));

        collection.updateOne(json(""), json("$push: {value: {$each: ['a', 'b', 'c'], $slice: -2}}"));

        assertThat(collection.find())
            .containsExactly(json("_id: 1, value: ['b', 'c']"));

        collection.updateOne(json(""), json("$push: {value: {$each: [1, 2], $slice: -5}}"));

        assertThat(collection.find())
            .containsExactly(json("_id: 1, value: ['b', 'c', 1, 2]"));

        collection.updateOne(json(""), json("$push: {value: {$each: ['x', 'y', 'z'], $slice: 0}}"));

        assertThat(collection.find())
            .containsExactly(json("_id: 1, value: []"));
    }

    @Test
    void testUpdatePushSort() {
        collection.insertOne(json("_id: 1"));

        collection.updateOne(json(""), json("$push: {value: {$each: [1, 5, 6, 3], $sort: -1}}"));
        assertThat(collection.find()).containsExactly(json("_id: 1, value: [6, 5, 3, 1]"));

        collection.updateOne(json(""), json("$push: {value: {$each: [{value: 1}, {value: 3}, {value: 2}], $sort: {value: 1}}}"));
        assertThat(collection.find()).containsExactly(json("_id: 1, value: [6, 5, 3, 1, {value: 1}, {value: 2}, {value: 3}]"));
    }

    @Test
    public void testUpdatePushSortAndSlice() {
        collection.insertOne(json("_id: 1, quizzes: [{wk: 2, score: 9}, {wk: 1, score: 10}]"));

        collection.updateOne(json(""), json("$push: {quizzes: {" +
            "$each: [{wk: 5, score: 8}, {wk: 6, score: 7}, {wk: 7, score: 6}]," +
            "$sort: { score: -1 }," +
            "$slice: 3}}"));

        assertThat(collection.find())
            .containsExactly(
                json("_id: 1, quizzes: [" +
                    "{wk: 1, score: 10}," +
                    "{wk: 2, score: 9}," +
                    "{wk: 5, score: 8}" +
                    "]")
            );
    }

    @Test
    void testUpdatePushPosition() {
        collection.insertOne(json("_id: 1, value: [1, 2]"));

        collection.updateOne(json(""), json("$push: {value: {$each: [3, 4], $position: 10}}"));
        assertThat(collection.find()).containsExactly(json("_id: 1, value: [1, 2, 3, 4]"));

        collection.updateOne(json(""), json("$push: {value: {$each: ['x'], $position: 2}}"));
        assertThat(collection.find()).containsExactly(json("_id: 1, value: [1, 2, 'x', 3, 4]"));

        collection.updateOne(json(""), json("$push: {value: {$each: ['y'], $position: -2}}"));
        assertThat(collection.find()).containsExactly(json("_id: 1, value: [1, 2, 'x', 'y', 3, 4]"));
    }

    @Test
    void testUpdatePushEach_unknownModifier() {
        collection.insertOne(json("_id: 1"));

        assertMongoWriteException(() -> collection.updateOne(json("_id: 1"), json("$push: {value: {$each: [1, 2, 3], $illegal: 1}}")),
            2, "BadValue", "Unrecognized clause in $push: $illegal");
    }

    @Test
    void testUpdatePushEach_illegalOptions() {
        collection.insertOne(json("_id: 1"));

        assertMongoWriteException(() -> collection.updateOne(json(""), json("$push: {value: {$each: [1, 2, 3], $slice: 'abc'}}")),
            2, "BadValue", "The value for $slice must be an integer value but was given type: string");

        assertMongoWriteException(() -> collection.updateOne(json(""), json("$push: {value: {$each: [1, 2, 3], $sort: 'abc'}}")),
            2, "BadValue", "The $sort is invalid: use 1/-1 to sort the whole element, or {field:1/-1} to sort embedded fields");

        assertMongoWriteException(() -> collection.updateOne(json(""), json("$push: {value: {$each: [1, 2, 3], $position: 'abc'}}")),
            2, "BadValue", "The value for $position must be an integer value, not of type: string");
    }

    @Test
    void testUpdatePushAll() {
        collection.insertOne(json("_id: 1"));

        assertMongoWriteException(() -> collection.updateOne(json(""), json("$pushAll: {field: 'value'}")),
            9, "FailedToParse", "Unknown modifier: $pushAll. Expected a valid update modifier or pipeline-style update specified as an array");
    }

    @Test
    void testUpdateAddToSet() {
        Document idObj = json("_id: 1");
        collection.insertOne(idObj);
        collection.updateOne(idObj, json("$addToSet: {'field.subfield.subsubfield': 'value'}"));
        assertThat(collection.find(idObj).first()).isEqualTo(json("_id: 1, field: {subfield: {subsubfield: ['value']}}"));

        // addToSet to non-array
        collection.updateOne(idObj, json("$set: {field: 'value'}"));
        assertThatExceptionOfType(MongoWriteException.class)
            .isThrownBy(() -> collection.updateOne(idObj, json("$addToSet: {field: 'value'}")))
            .withMessageContaining("Cannot apply $addToSet to non-array field. Field named 'field' has non-array type string");

        // addToSet with multiple fields

        collection.updateOne(idObj, json("$addToSet: {field1: 'value', field2: 'value2'}"));

        assertThat(collection.find(idObj).first())
            .isEqualTo(json("_id: 1, field: 'value', field1: ['value'], field2: ['value2']"));

        // addToSet duplicate
        collection.updateOne(idObj, json("$addToSet: {field1: 'value'}"));
        assertThat(collection.find(idObj).first())
            .isEqualTo(json("_id: 1, field: 'value', field1: ['value'], field2: ['value2']"));
    }

    @Test
    void testUpdateAddToSetEach() {
        collection.insertOne(json("_id: 1"));

        collection.updateOne(json("_id: 1"), addEachToSet("a", List.of(6, 5, 4)));
        assertThat(collection.find()).containsExactly(json("_id: 1, a: [6, 5, 4]"));

        collection.updateOne(json("_id: 1"), addEachToSet("a", List.of(3, 2, 1)));
        assertThat(collection.find()).containsExactly(json("_id: 1, a: [6, 5, 4, 3, 2, 1]"));

        collection.updateOne(json("_id: 1"), addEachToSet("a", List.of(7, 7, 9, 2)));
        assertThat(collection.find()).containsExactly(json("_id: 1, a: [6, 5, 4, 3, 2, 1, 7, 9]"));

        collection.updateOne(json("_id: 1"), addEachToSet("a", List.of(12, 13, 12)));
        assertThat(collection.find()).containsExactly(json("_id: 1, a: [6, 5, 4, 3, 2, 1, 7, 9, 12, 13]"));

        collection.replaceOne(json("_id: 1"), json("_id: 1"));

        collection.updateOne(json("_id: 1"), json("$addToSet: {value: {key: 'x'}}"));
        assertThat(collection.find()).containsExactly(json("_id: 1, value: [{key: 'x'}]"));
    }

    @Test
    void testUpdateAddToSetEach_unknownModifier() {
        collection.insertOne(json("_id: 1"));

        assertMongoWriteException(() -> collection.updateOne(json("_id: 1"), json("$addToSet: {value: {$each: [1, 2, 3], $slice: 2}}")),
            2, "BadValue", "Found unexpected fields after $each in $addToSet: { $each: [ 1, 2, 3 ], $slice: 2 }");

        assertMongoWriteException(() -> collection.updateOne(json("_id: 1"), json("$addToSet: {value: {$each: [1, 2, 3], value: 2}}")),
            2, "BadValue", "Found unexpected fields after $each in $addToSet: { $each: [ 1, 2, 3 ], value: 2 }");

        collection.updateOne(json("_id: 1"), json("$addToSet: {value: {key: 2, $each: [1, 2, 3]}}"));

        assertThat(collection.find())
            .containsExactly(json("_id: 1, value: [{key: 2, $each: [1, 2, 3]}]"));

        collection.updateOne(json("_id: 1"), json("$addToSet: {value: {$slice: 2, $each: [1, 2, 3]}}"));

        assertThat(collection.find())
            .containsExactly(json("_id: 1, value: [{key: 2, $each: [1, 2, 3]}, {$slice: 2, $each: [1, 2, 3]}]"));

    }

    protected String getExpectedPathPrefix_testUpdateAddToSetEach_unknownModifier() {
        return "value.";
    }

    @Test
    void testUpdateDatasize() {
        Document obj = json("_id: 1, a: {x: [1, 2, 3]}");
        collection.insertOne(obj);
        Number oldSize = getCollStats().getInteger("size");

        collection.updateOne(json("_id: 1"), set("a.x.0", 3));
        assertThat(collection.find().first().get("a")).isEqualTo(json("x: [3, 2, 3]"));
        Number newSize = getCollStats().getInteger("size");
        assertThat(newSize).isEqualTo(oldSize);

        // now increase the db
        collection.updateOne(json("_id: 1"), set("a.x.0", "abc"));
        Number yetNewSize = getCollStats().getInteger("size");
        assertThat(yetNewSize.intValue() - oldSize.intValue()).isEqualTo(4);
    }

    @Test
    void testUpdatePull() {
        Document obj = json("_id: 1");
        collection.insertOne(obj);

        // pull from non-existing field
        collection.updateOne(obj, json("$pull: {field1: 'value2', field2: 'value3'}"));

        assertThat(collection.find(obj).first()).isEqualTo(obj);

        // pull from non-array
        collection.updateOne(obj, set("field", "value"));

        assertMongoWriteException(() -> collection.updateOne(obj, pull("field", "value")),
            2, "BadValue", "Cannot apply $pull to a non-array value");

        // pull standard
        collection.updateOne(obj, json("$set: {field: ['value1', 'value2', 'value1']}"));

        collection.updateOne(obj, pull("field", "value1"));

        assertThat(collection.find(obj).first().get("field")).isEqualTo(List.of("value2"));

        // pull with multiple fields

        collection.updateOne(obj, json("$set: {field1: ['value1', 'value2', 'value1']}"));
        collection.updateOne(obj, json("$set: {field2: ['value3', 'value3', 'value1']}"));

        collection.updateOne(obj, json("$pull: {field1: 'value2', field2: 'value3'}"));

        assertThat(collection.find(obj).first().get("field1")).isEqualTo(List.of("value1", "value1"));
        assertThat(collection.find(obj).first().get("field2")).isEqualTo(List.of("value1"));
    }

    @Test
    void testUpdatePullValueWithCondition() {
        collection.insertOne(json("_id: 1, votes: [ 3, 5, 6, 7, 7, 8 ]"));
        collection.updateOne(json("_id: 1"), json("$pull: {votes: {$gte: 6}}"));

        assertThat(collection.find().first()).isEqualTo(json("_id: 1, votes: [3, 5]"));
    }

    @Test
    void testUpdatePullDocuments() {
        collection.insertOne(json("_id: 1, results: [{item: 'A', score: 5}, {item: 'B', score: 8, comment: 'foobar'}]"));
        collection.insertOne(json("_id: 2, results: [{item: 'C', score: 8, comment: 'foobar'}, {item: 'B', score: 4}]"));

        collection.updateOne(json(""), json("$pull: {results: {score: 8 , item: 'B'}}"));

        assertThat(collection.find(json("_id: 1")).first()).isEqualTo(json("_id: 1, results: [{item: 'A', score: 5}]"));
        assertThat(collection.find(json("_id: 2")).first()).isEqualTo(json("_id: 2, results: [{item: 'C', score: 8, comment: 'foobar'}, {item: 'B', score: 4}]"));
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/20
    @Test
    void testUpdatePullLeavesEmptyArray() {
        Document obj = json("_id: 1");
        collection.insertOne(obj);
        collection.updateOne(obj, json("$set: {field: [{'key1': 'value1', 'key2': 'value2'}]}"));
        collection.updateOne(obj, json("$pull: {field: {'key1': 'value1'}}"));

        assertThat(collection.find(obj).first()).isEqualTo(json("_id: 1, field: []"));
    }

    @Test
    void testUpdatePullAll() {
        Document obj = json("_id: 1");
        collection.insertOne(obj);
        collection.updateOne(obj, json("$set: {field: 'value'}"));
        assertMongoWriteException(() -> collection.updateOne(obj, json("$pullAll: {field: 'value'}")),
            2, "BadValue", "$pullAll requires an array argument but was given a string");

        collection.updateOne(obj, json("$set: {field1: ['value1', 'value2', 'value1', 'value3', 'value4', 'value3']}"));

        collection.updateOne(obj, json("$pullAll: {field1: ['value1', 'value3']}"));

        assertThat(collection.find(obj).first().get("field1")).isEqualTo(List.of("value2", "value4"));

        assertMongoWriteException(() -> collection.updateOne(obj, json("$pullAll: {field1: 'bar'}")),
            2, "BadValue", "$pullAll requires an array argument but was given a string");
    }

    @Test
    void testUpdatePullAll_Documents() {
        collection.insertOne(json("_id: 1, persons: [{id: 1}, {id: 2}, {id: 5}, {id: 5}, {id: 1}, {id: 0}]"));

        collection.updateOne(json("_id: 1"), json("$pullAll: {persons: [{id: 0.0}, {id: 5}]}"));

        assertThat(collection.find(json("")))
            .containsExactly(json("_id: 1, persons: [{id: 1}, {id: 2}, {id: 1}]"));
    }

    @Test
    void testUpdateSet() {
        Document object = json("_id: 1");

        collection.insertOne(object);
        assertThat(collection.find(object).first()).isEqualTo(object);

        collection.updateOne(object, json("$set: {foo: 'bar'}"));

        Document expected = json("");
        expected.putAll(object);
        expected.put("foo", "bar");

        collection.updateOne(object, json("$set: {bar: 'bla'}"));
        expected.put("bar", "bla");
        assertThat(collection.find(object).first()).isEqualTo(expected);

        assertMongoWriteException(() -> collection.updateOne(object, json("$set: {'foo.bar': 'bla'}")),
            28, "PathNotViable", "Cannot create field 'bar' in element {foo: \"bar\"}");
        assertThat(collection.find(object).first()).isEqualTo(expected);

        collection.updateOne(object, json("$set: {'other.foo': '123'}"));
        expected.putAll(json("other: {foo: '123'}"));
        assertThat(collection.find(object).first()).isEqualTo(expected);

        collection.updateOne(object, json("$set: {'other.foo': 42}"));
        expected.putAll(json("other: {foo: 42}"));
        assertThat(collection.find(object).first()).isEqualTo(expected);

        collection.updateOne(object, json("$set: {'other.bar': 'x'}"));
        expected.putAll(json("other: {foo: 42, bar: 'x'}"));
        assertThat(collection.find(object).first()).isEqualTo(expected);

        collection.updateOne(object, json("$set: {'other.bar': null}"));
        expected.putAll(json("other: {foo: 42, bar: null}"));
        assertThat(collection.find(object).first()).isEqualTo(expected);

        collection.updateOne(object, json("$set: {'other.missing': null}"));
        expected.putAll(json("other: {foo: 42, bar: null, missing: null}"));
        assertThat(collection.find(object).first()).isEqualTo(expected);

        collection.updateOne(object, json("$set: {'other': null}"));
        expected.putAll(json("other: null"));
        assertThat(collection.find(object).first()).isEqualTo(expected);
    }

    @Test
    void testUpdateSet_arrayOfDocuments() {
        collection.insertOne(json("_id: 1, foo: [{bar: 1}, {bar: 2}]"));

        assertMongoWriteException(() -> collection.updateOne(json("_id: 1"), json("$set: {'foo.bar': 3}")),
            28, "PathNotViable", "Cannot create field 'bar' in element {foo: [ { bar: 1 }, { bar: 2 } ]}");
    }

    @Test
    void testUpdateSetOnInsert() {
        Document object = json("_id: 1");
        collection.updateOne(object, json("$set: {b: 3}, $setOnInsert: {a: 3}"), new UpdateOptions().upsert(true));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, b: 3, a: 3"));

        collection.updateOne(object, json("$set: {b: 4}, $setOnInsert: {a: 5}"), new UpdateOptions().upsert(true));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, b: 4, a: 3")); // 'a' is unchanged
    }

    @Test
    void testUpdateSetWithArrayIndices() {

        // SERVER-181

        collection.insertOne(json("_id: 1, a: [{x: 0}]"));
        collection.updateOne(json(""), json("$set: {'a.0.x': 3}"));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, a: [{x: 3}]"));

        collection.updateOne(json(""), json("$set: {'a.1.z': 17}"));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, a: [{x: 3}, {z: 17}]"));

        collection.updateOne(json(""), json("$set: {'a.0.y': 7}"));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, a: [{x: 3, y: 7}, {z: 17}]"));

        collection.updateOne(json(""), json("$set: {'a.1': 'test'}"));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, a: [{x: 3, y: 7}, 'test']"));
    }

    @Test
    void testUpdateUnsetWithArrayIndices() {

        // SERVER-273

        collection.insertOne(json("_id: 1, a: [{x: 0}]"));
        collection.updateOne(json(""), json("$unset: {'a.0.x': 1}"));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, a: [{}]"));

        collection.updateOne(json(""), json("$unset: {'a.0': 1}"));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, a: [null]"));

        collection.updateOne(json(""), json("$unset: {'a.10': 1}"));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, a: [null]"));
    }

    @Test
    void testUpdateMax() {
        Document object = json("_id: 1");

        collection.insertOne(object);

        collection.updateOne(object, json("$max: {'foo.bar': 1}"));
        assertThat(collection.find(object).first()).isEqualTo(json("_id: 1, foo : {bar : 1}"));

        collection.updateOne(object, json("$max: {'foo.bar': 1}"));
        assertThat(collection.find(object).first()).isEqualTo(json("_id: 1, foo : {bar : 1}"));

        collection.updateOne(object, json("$max: {'foo.bar': 10}"));
        assertThat(collection.find(object).first()).isEqualTo(json("_id: 1, foo : {bar : 10}"));

        collection.updateOne(object, json("$max: {'foo.bar': -100}"));
        assertThat(collection.find(object).first()).isEqualTo(json("_id: 1, foo : {bar : 10}"));

        collection.updateOne(object, json("$max: {'foo.bar': '1'}"));
        assertThat(collection.find(object).first()).isEqualTo(json("_id: 1, foo : {bar : '1'}"));

        collection.updateOne(object, json("$max: {'foo.bar': null}"));
        assertThat(collection.find(object).first()).isEqualTo(json("_id: 1, foo : {bar : '1'}"));

        collection.updateOne(object, json("$max: {'foo.bar': '2', 'buz' : 1}"));
        assertThat(collection.find(object).first()).isEqualTo(json("_id: 1, foo : {bar : '2'}, buz : 1"));
    }

    @Test
    void testUpdateMin() {
        Document object = json("_id: 1");

        collection.insertOne(object);

        collection.updateOne(object, json("$min: {'foo.bar': 'b'}"));
        assertThat(collection.find(object).first()).isEqualTo(json("_id: 1, foo : {bar : 'b'}"));

        collection.updateOne(object, json("$min: {'foo.bar': 'a'}"));
        assertThat(collection.find(object).first()).isEqualTo(json("_id: 1, foo : {bar : 'a'}"));

        collection.updateOne(object, json("$min: {'foo.bar': 10}"));
        assertThat(collection.find(object).first()).isEqualTo(json("_id: 1, foo : {bar : 10}"));

        collection.updateOne(object, json("$min: {'foo.bar': 10}"));
        assertThat(collection.find(object).first()).isEqualTo(json("_id: 1, foo : {bar : 10}"));

        collection.updateOne(object, json("$min: {'foo.bar': 1}"));
        assertThat(collection.find(object).first()).isEqualTo(json("_id: 1, foo : {bar : 1}"));

        collection.updateOne(object, json("$min: {'foo.bar': 100}"));
        assertThat(collection.find(object).first()).isEqualTo(json("_id: 1, foo : {bar : 1}"));

        collection.updateOne(object, json("$min: {'foo.bar': null}"));
        assertThat(collection.find(object).first()).isEqualTo(json("_id: 1, foo : {bar : null}"));

        collection.updateOne(object, json("$min: {'foo.bar': 'a'}"));
        assertThat(collection.find(object).first()).isEqualTo(json("_id: 1, foo : {bar : null}"));
    }

    @Test
    void testUpdateMinMaxWithLists() {
        collection.insertOne(json("_id: 1, a: [1, 2], b: [3, 4]"));
        collection.insertOne(json("_id: 2"));
        collection.insertOne(json("_id: 3, a: null, b: null"));
        collection.insertOne(json("_id: 4, a: 'abc', b: 'xyz'"));
        collection.insertOne(json("_id: 5, a: 1, b: 2"));

        collection.updateMany(json(""), json("$min: {a: [2, 3], b: [1, 2]}"));

        assertThat(collection.find(json("")))
            .containsExactlyInAnyOrder(
                json("_id: 1, a: [1, 2], b: [1, 2]"),
                json("_id: 2, a: [2, 3], b: [1, 2]"),
                json("_id: 3, a: null, b: null"),
                json("_id: 4, a: 'abc', b: 'xyz'"),
                json("_id: 5, a: 1, b: 2")
            );

        collection.updateMany(json(""), json("$max: {a: [1, 3], b: [2, 3]}"));

        assertThat(collection.find(json("")))
            .containsExactlyInAnyOrder(
                json("_id: 1, a: [1, 3], b: [2, 3]"),
                json("_id: 2, a: [2, 3], b: [2, 3]"),
                json("_id: 3, a: [1, 3], b: [2, 3]"),
                json("_id: 4, a: [1, 3], b: [2, 3]"),
                json("_id: 5, a: [1, 3], b: [2, 3]")
            );
    }

    // see http://docs.mongodb.org/manual/reference/operator/update/max
    @Test
    void testUpdateMaxCompareNumbers() {
        Document object = json("_id: 1, highScore: 800, lowScore: 200");

        collection.insertOne(object);

        collection.updateOne(json("_id: 1"), json("$max: {highScore: 950}"));
        assertThat(collection.find(json("_id: 1")).first()).isEqualTo(json("_id: 1, highScore: 950, lowScore: 200"));

        collection.updateOne(json("_id: 1"), json("$max: {highScore: 870}"));
        assertThat(collection.find(json("_id: 1")).first()).isEqualTo(json("_id: 1, highScore: 950, lowScore: 200"));
    }

    // see http://docs.mongodb.org/manual/reference/operator/update/max
    @Test
    void testUpdateMaxCompareDates() {
        Document object = new Document("_id", 1).append("desc", "crafts")
            .append("dateEntered", instant("2013-10-01T05:00:00Z"))
            .append("dateExpired", instant("2013-10-01T16:38:16Z"));

        collection.insertOne(object);

        collection.updateOne(json("_id: 1"),
            new Document("$max", new Document("dateExpired", instant("2013-09-30T00:00:00Z"))));
        assertThat(collection.find(json("_id: 1")).first())
            .isEqualTo(json("_id: 1, desc: 'crafts'")
                .append("dateEntered", date("2013-10-01T05:00:00Z"))
                .append("dateExpired", date("2013-10-01T16:38:16Z")));

        collection.updateOne(json("_id: 1"),
            new Document("$max", new Document("dateExpired", instant("2014-01-07T00:00:00Z"))));
        assertThat(collection.find(json("_id: 1")).first()).isEqualTo(
            json("_id: 1, desc: 'crafts'")
                .append("dateEntered", date("2013-10-01T05:00:00Z"))
                .append("dateExpired", date("2014-01-07T00:00:00Z")));
    }

    // see http://docs.mongodb.org/manual/reference/operator/update/min
    @Test
    void testUpdateMinCompareNumbers() {
        Document object = json("_id: 1, highScore: 800, lowScore: 200");

        collection.insertOne(object);

        collection.updateOne(json("_id: 1"), json("$min: {lowScore: 150}"));
        assertThat(collection.find(json("_id: 1")).first()).isEqualTo(json("_id: 1, highScore: 800, lowScore: 150"));

        collection.updateOne(json("_id: 1"), json("$min: {lowScore: 250}"));
        assertThat(collection.find(json("_id: 1")).first()).isEqualTo(json("_id: 1, highScore: 800, lowScore: 150"));
    }

    // see http://docs.mongodb.org/manual/reference/operator/update/min
    @Test
    void testUpdateMinCompareDates() {
        Document object = new Document("_id", 1).append("desc", "crafts")
            .append("dateEntered", instant("2013-10-01T05:00:00Z"))
            .append("dateExpired", instant("2013-10-01T16:38:16Z"));

        collection.insertOne(object);

        collection.updateOne(json("_id: 1"),
            new Document("$min", new Document("dateEntered", instant("2013-09-25T00:00:00Z"))));
        assertThat(collection.find(json("_id: 1")).first()) //
            .isEqualTo(json("_id: 1, desc: 'crafts'") //
                .append("dateEntered", date("2013-09-25T00:00:00Z")) //
                .append("dateExpired", date("2013-10-01T16:38:16Z")));

        collection.updateOne(json("_id: 1"),
            new Document("$min", new Document("dateEntered", instant("2014-01-07T00:00:00Z"))));
        assertThat(collection.find(json("_id: 1")).first()) //
            .isEqualTo(json("_id: 1, desc: 'crafts'") //
                .append("dateEntered", date("2013-09-25T00:00:00Z")) //
                .append("dateExpired", date("2013-10-01T16:38:16Z")));
    }

    @Test
    void testUpdatePop() {
        Document object = json("_id: 1");

        collection.insertOne(object);
        collection.updateOne(object, json("$pop: {'foo.bar': 1}"));

        assertThat(collection.find(object).first()).isEqualTo(object);
        collection.updateOne(object, json("$set: {'foo.bar': [1, 2, 3]}"));
        assertThat(collection.find(object).first()).isEqualTo(json("_id: 1, foo: {bar: [1, 2, 3]}"));

        collection.updateOne(object, json("$pop: {'foo.bar': 1.0}"));
        assertThat(collection.find(object).first()).isEqualTo(json("_id: 1, foo: {bar: [1, 2]}"));

        collection.updateOne(object, json("$pop: {'foo.bar': -1}"));
        assertThat(collection.find(object).first()).isEqualTo(json("_id: 1, foo: {bar: [2]}"));

        assertMongoWriteException(() -> collection.updateOne(object, json("$pop: {'foo.bar': null}")),
            9, "FailedToParse", "Expected a number in: foo.bar: null");

        assertMongoWriteException(() -> collection.updateOne(object, json("$pop: {'foo.bar': 'x'}")),
            9, "FailedToParse", "Expected a number in: foo.bar: \"x\"");

        assertMongoWriteException(() -> collection.updateOne(object, json("$pop: {'foo.bar': 2}")),
            9, "FailedToParse", "$pop expects 1 or -1, found: 2");
    }

    @Test
    void testUpdateUnset() {
        Document obj = json("_id: 1, a: 1, b: null, c: 'value'");
        collection.insertOne(obj);
        assertMongoWriteException(() -> collection.updateOne(obj, json("$unset: {_id: ''}")),
            66, "ImmutableField", "Performing an update on the path '_id' would modify the immutable field '_id'");

        collection.updateOne(json("_id: 1"), json("$unset: {'a.b.z': 1}"));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, a: 1, b: null, c: 'value'"));

        collection.updateOne(obj, json("$unset: {a: '', b: ''}"));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, c: 'value'"));

        collection.updateOne(obj, Updates.unset("c.y"));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, c: 'value'"));

        collection.replaceOne(json("_id: 1"), json("a: {b: 'foo', c: 'bar'}"));

        collection.updateOne(json("_id: 1"), json("$unset: {'a.b': 1}"));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, a: {c: 'bar'}"));

        collection.updateOne(json("_id: 1"), json("$unset: {'a.b.z': 1}"));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, a: {c: 'bar'}"));
    }

    @Test
    void testUpdateWithIdIn() {
        collection.insertOne(json("_id: 1"));
        Document update = json("$push: {n: {_id: 2, u: 3}}, $inc: {c: 4}");
        Document expected = json("_id: 1, n: [{_id: 2, u: 3}], c: 4");
        collection.updateOne(json("_id: {$in: [1]}"), update);
        assertThat(collection.find().first()).isEqualTo(expected);
    }

    @Test
    void testUpdateMulti() {
        collection.insertOne(json("a: 1"));
        collection.insertOne(json("a: 1"));
        UpdateResult result = collection.updateOne(json("a: 1"), json("$set: {b: 2}"));

        assertThat(result.getModifiedCount()).isEqualTo(1);

        assertThat(collection.countDocuments(new Document("b", 2))).isEqualTo(1);

        result = collection.updateMany(json("a: 1"), json("$set: {b: 3}"));
        assertThat(result.getModifiedCount()).isEqualTo(2);
        assertThat(collection.countDocuments(new Document("b", 2))).isEqualTo(0);
        assertThat(collection.countDocuments(new Document("b", 3))).isEqualTo(2);
    }

    @Test
    void testUpdateIllegalInt() {
        collection.insertOne(json("_id: 1, a: {x: 1}"));

        assertMongoWriteException(() -> collection.updateOne(json("_id: 1"), json("$inc: {a: 1}")),
            14, "TypeMismatch", "Cannot apply $inc to a value of non-numeric type. {_id: 1} has the field 'a' of non-numeric type object");

        assertThatExceptionOfType(MongoServerException.class)
            .isThrownBy(() -> collection.updateOne(json("_id: 1"), json("$inc: {'a.x': 'b'}")))
            .withMessageContaining("Cannot increment with non-numeric argument: {a.x: \"b\"}");
    }

    @Test
    void testUpdateWithIdInMulti() {
        collection.insertMany(List.of(json("_id: 1"), json("_id: 2")));
        collection.updateMany(json("_id: {$in: [1, 2]}"), json("$set: {n: 1}"));
        assertThat(collection.find())
            .containsExactly(
                json("_id: 1, n: 1"),
                json("_id: 2, n: 1")
            );
    }

    @Test
    void testUpdateWithIdInMultiReturnModifiedDocumentCount() {
        collection.insertMany(List.of(json("_id: 1"), json("_id: 2")));
        UpdateResult result = collection.updateMany(json("_id: {$in: [1, 2]}"), json("$set: {n: 1}"));
        assertThat(result.getModifiedCount()).isEqualTo(2);
    }

    @Test
    void testUpdateWithIdQuery() {
        collection.insertMany(List.of(json("_id: 1"), json("_id: 2")));
        collection.updateMany(json("_id: {$gt:1}"), json("$set: {n: 1}"));
        assertThat(collection.find())
            .containsExactly(json("_id: 1"), json("_id: 2, n: 1"));
    }

    @Test
    void testUpdateWithObjectId() {
        collection.insertOne(json("_id: {n: 1}"));
        UpdateResult result = collection.updateOne(json("_id: {n: 1}"), json("$set: {a: 1}"));
        assertThat(result.getModifiedCount()).isEqualTo(1);
        assertThat(collection.find().first()).isEqualTo(json("_id: {n: 1}, a: 1"));
    }

    @Test
    void testUpdateArrayMatch() {

        collection.insertOne(json("_id: 1, a: [{x: 1, y: 1}, {x: 2, y: 2}, {x: 3, y: 3}]"));

        collection.updateOne(json("'a.x': 2"), json("$inc: {'a.$.y': 1}"));

        assertThat(collection.find(json("'a.x': 2")).first()).isEqualTo(json("_id: 1, a: [{x: 1, y: 1}, {x: 2, y: 3}, {x: 3, y: 3}]"));

        collection.insertOne(json("'array': [{'123a': {'name': 'old'}}]"));
        assertThat(collection.find(json("'array.123a.name': 'old'")).first()).isNotNull();
        collection.updateOne(json("'array.123a.name': 'old'"), json("$set: {'array.$.123a.name': 'new'}"));
        assertThat(collection.find(json("'array.123a.name': 'new'")).first()).isNotNull();
        assertThat(collection.find(json("'array.123a.name': 'old'")).first()).isNull();
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/85
    @Test
    void testUpdateArrayMatch_MultipleFields() {
        collection.insertOne(json("_id: 1, a: [{x: 1, y: 1}, {x: 2, y: 2}, {x: 3, y: 3}]"));

        collection.updateOne(json("'a.x': 2"),
            json("$inc: {'a.$.y': 1, 'a.$.x': 1}, $set: {'a.$.foo': 1, 'a.$.foo2': 1}"));

        assertThat(collection.find(json("")))
            .containsExactly(json("_id: 1, a: [{x: 1, y: 1}, {x: 3, y: 3, foo: 1, foo2: 1}, {x: 3, y: 3}]"));
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/113
    @Test
    void testUpdateArrayMatch_updateMany() {
        collection.insertOne(json("_id: 1, grades: [{id: 1, value: 90}]"));
        collection.insertOne(json("_id: 2, grades: [{id: 1, value: 85}, {id: 2, value: 80}, {id: 3, value: 80}]"));
        collection.insertOne(json("_id: 3, grades: [{id: 1, value: 50}, {id: 1, value: 80}]"));
        collection.insertOne(json("_id: 4"));

        collection.updateMany(json("'grades.value': 80"), json("$set: {'grades.$.value': 82, 'grades.$.changed': true}"));

        assertThat(collection.find().sort(json("_id: 1")))
            .containsExactly(
                json("_id: 1, grades: [{id: 1, value: 90}]"),
                json("_id: 2, grades: [{id: 1, value: 85}, {id: 2, value: 82, changed: true}, {id: 3, value: 80}]"),
                json("_id: 3, grades: [{id: 1, value: 50}, {id: 1, value: 82, changed: true}]"),
                json("_id: 4")
            );
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/232
    @Test
    void testUpdateArrayMatch_ObjectId() {
        collection.insertOne(json("_id: 1")
            .append("myArray", List.of(new Document("_id", new ObjectId(123, 456)))));

        UpdateResult updateResult = collection.updateOne(
            and(eq("_id", 1), eq("myArray._id", new ObjectId(123, 456))),
            set("myArray.$.name", "new name")
        );

        assertThat(updateResult.getMatchedCount()).isEqualTo(1);
        assertThat(updateResult.getModifiedCount()).isEqualTo(1);
        assertThat(updateResult.getUpsertedId()).isNull();

        assertThat(collection.find())
            .containsExactly(json("_id: 1")
                .append("myArray", List.of(
                    new Document("_id", new ObjectId(123, 456))
                        .append("name", "new name")
                )));
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/32
    @Test
    void testUpdateWithNotAndSizeOperator() {
        collection.insertOne(json("_id: 1, array: ['a', 'b']"));
        collection.insertOne(json("_id: 2, array: ['b']"));
        collection.insertOne(json("_id: 3, array: ['a']"));

        collection.updateMany(json("array: {$not: {$size: 1}}"), json("$pull: {array: 'a'}"));

        assertThat(collection.find())
            .containsExactlyInAnyOrder(
                json("_id: 1, array: ['b']"),
                json("_id: 2, array: ['b']"),
                json("_id: 3, array: ['a']")
            );
    }

    @Test
    void testMultiUpdateArrayMatch() {
        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2, x: [1, 2, 3]"));
        collection.insertOne(json("_id: 3, x: 99"));

        collection.updateMany(json("x: 2"), json("$inc: {'x.$': 1}"));

        assertThat(collection.find().sort(json("_id: 1")))
            .containsExactly(
                json("_id: 1"),
                json("_id: 2, x: [1, 3, 3]"),
                json("_id: 3, x: 99")
            );
    }

    @Test
    void testUpsert() {
        UpdateResult result = collection.updateMany(json("n:'jon'"), json("$inc: {a: 1}"), new UpdateOptions().upsert(true));
        assertThat(result.getModifiedCount()).isEqualTo(0);

        Document object = collection.find().first();
        assertThat(result.getUpsertedId()).isEqualTo(new BsonObjectId(object.getObjectId("_id")));

        object.remove("_id");
        assertThat(object).isEqualTo(json("n: 'jon', a: 1"));

        result = collection.updateOne(json("_id: 17, n: 'jon'"), json("$inc: {a: 1}"), new UpdateOptions().upsert(true));
        assertThat(result.getUpsertedId()).isEqualTo(new BsonInt32(17));
        assertThat(collection.find(json("_id: 17")).first()).isEqualTo(json("_id: 17, n: 'jon', a: 1"));
    }

    @Test
    void testUpsertFieldOrder() {
        collection.updateOne(json("'x.y': 2"), json("$inc: {a: 7}"), new UpdateOptions().upsert(true));
        Document obj = collection.find().first();
        obj.remove("_id");
        // this actually differs from the official MongoDB implementation
        assertThat(obj).isEqualTo(json("x: {y: 2}, a: 7"));
    }

    @Test
    void testUpsertWithoutId() {
        UpdateResult result = collection.updateOne(eq("a", 1), set("a", 2), new UpdateOptions().upsert(true));
        assertThat(result.getModifiedCount()).isEqualTo(0);
        assertThat(result.getUpsertedId()).isNotNull();
        assertThat(collection.find().first().get("_id")).isInstanceOf(ObjectId.class);
        assertThat(collection.find().first().get("a")).isEqualTo(2);
    }

    @Test
    void testUpsertOnIdWithPush() {
        Document update1 = json("$push: {c: {a: 1, b: 2}}");
        Document update2 = json("$push: {c: {a: 3, b: 4}}");

        collection.updateOne(json("_id: 1"), update1, new UpdateOptions().upsert(true));

        collection.updateOne(json("_id: 1"), update2, new UpdateOptions().upsert(true));

        Document expected = json("_id: 1, c: [{a: 1, b: 2}, {a: 3, b: 4}]");

        assertThat(collection.find(json("'c.a':3, 'c.b':4")).first()).isEqualTo(expected);
    }

    @Test
    void testUpsertWithConditional() {
        Document query = json("_id: 1, b: {$gt: 5}");
        Document update = json("$inc: {a: 1}");
        UpdateResult updateResult = collection.updateOne(query, update, new UpdateOptions().upsert(true));
        assertThat(updateResult.getModifiedCount()).isZero();
        assertThat(updateResult.getMatchedCount()).isZero();
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, a: 1"));
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/29
    @Test
    void testUpsertWithoutChange() {
        collection.insertOne(json("_id: 1, a: 2, b: 3"));
        Document query = json("_id: 1");
        Document update = json("$set: {a: 2}");
        UpdateResult updateResult = collection.updateOne(query, update, new UpdateOptions().upsert(true));
        assertThat(updateResult.getModifiedCount()).isZero();
        assertThat(updateResult.getMatchedCount()).isOne();
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, a: 2, b: 3"));
    }

    @Test
    void testUpsertWithEmbeddedQuery() {
        collection.updateOne(json("_id: 1, 'e.i': 1"), json("$set: {a: 1}"), new UpdateOptions().upsert(true));
        assertThat(collection.find(json("_id: 1")).first()).isEqualTo(json("_id: 1, e: {i: 1}, a: 1"));
    }

    @Test
    void testUpsertWithIdIn() {
        Document query = json("_id: {$in: [1]}");
        Document update = json("$push: {n: {_id: 2 ,u : 3}}, $inc: {c: 4}");
        Document expected = json("_id: 1, n: [{_id: 2 ,u : 3}], c: 4");

        UpdateResult updateResult = collection.updateOne(query, update, new UpdateOptions().upsert(true));
        assertThat(updateResult.getModifiedCount()).isZero();
        assertThat(updateResult.getMatchedCount()).isZero();

        // the ID generation actually differs from official MongoDB which just
        // create a random object id
        Document actual = collection.find().first();
        assertThat(actual).isEqualTo(expected);
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/62
    @Test
    void testUpsertWithId() {
        Document query = json("somekey: 'somevalue'");
        Document update = json("$set: { _id: 'someid', somekey: 'some value' }");

        UpdateResult updateResult = collection.updateOne(query, update, new UpdateOptions().upsert(true));
        assertThat(updateResult.getModifiedCount()).isZero();
        assertThat(updateResult.getMatchedCount()).isZero();
        assertThat(updateResult.getUpsertedId()).isEqualTo(new BsonString("someid"));

        assertThat(collection.find())
            .containsExactly(json("_id: 'someid', somekey: 'some value' "));
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/62
    @Test
    void testUpsertWithId_duplicateKey() {
        collection.insertOne(json("_id: 'someid', somekey: 'other value'"));

        Document query = json("somekey: 'some value'");
        Document update = json("$set: { _id: 'someid', somekey: 'some value' }");

        assertMongoWriteException(() -> collection.updateOne(query, update, new UpdateOptions().upsert(true)),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: _id_ dup key: { _id: \"someid\" }");
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/93
    @Test
    void testReplaceOneWithId() {
        collection.replaceOne(json("_id: 1"), json("_id: 1, value: 'abc'"), new ReplaceOptions().upsert(true));

        assertThat(collection.find())
            .containsExactly(json("_id: 1, value: 'abc'"));

        collection.replaceOne(json("value: 'xyz'"), json("_id: 2, value: 'xyz'"), new ReplaceOptions().upsert(true));

        assertThat(collection.find())
            .containsExactly(
                json("_id: 1, value: 'abc'"),
                json("_id: 2, value: 'xyz'")
            );

        assertMongoWriteException(() -> collection.replaceOne(json("value: 'z'"), json("_id: 2, value: 'z'"), new ReplaceOptions().upsert(true)),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: _id_ dup key: { _id: 2 }");
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/154
    @Test
    void testReplaceOneWithIdAndRevision() {
        collection.insertOne(json("_id: 1, revision: 1"));
        collection.createIndex(json("revision: 1"));

        UpdateResult firstUpdateResult = collection.replaceOne(json("_id: 1, revision: 1"),
            json("_id: 1, revision: 2, value: 'abc'"));
        assertThat(firstUpdateResult.getModifiedCount()).isEqualTo(1);

        assertThat(collection.find())
            .containsExactly(json("_id: 1, revision: 2, value: 'abc'"));

        UpdateResult secondUpdateResult = collection.replaceOne(json("_id: 1, revision: 1"),
            json("_id: 1, revision: 3, value: 'xyz'"));
        assertThat(secondUpdateResult.getModifiedCount()).isZero();

        assertThat(collection.find())
            .containsExactly(json("_id: 1, revision: 2, value: 'abc'"));
    }

    @Test
    void testReplaceOneUpsertsWithGeneratedId() {
        collection.replaceOne(json("value: 'abc'"), json("value: 'abc'"), new ReplaceOptions().upsert(true));

        assertThat(collection.find())
            .extracting(document -> document.get("value"))
            .containsExactly("abc");

        assertThat(collection.find().first().get("_id"))
            .isInstanceOf(ObjectId.class);
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/227
    @Test
    void testReplaceWithEmptyDocument() {
        collection.insertOne(json("_id: 'myId', value: 'test'"));

        collection.replaceOne(json("_id: 'myId'"), json(""));

        assertThat(collection.find())
            .containsExactly(json("_id: 'myId'"));
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/41
    @Test
    void testBulkUpsert() {
        List<ReplaceOneModel<Document>> models = List.of(
            new ReplaceOneModel<>(Filters.eq("_id", 1), json("_id: 1, a: 1"), new ReplaceOptions().upsert(true)),
            new ReplaceOneModel<>(Filters.eq("_id", 2), json("_id: 2, a: 1"), new ReplaceOptions().upsert(true))
        );

        BulkWriteResult result = collection.bulkWrite(models, new BulkWriteOptions().ordered(false));
        assertThat(result.getUpserts())
            .extracting(BulkWriteUpsert::getId)
            .containsExactly(new BsonInt32(1), new BsonInt32(2));
        assertThat(result.getUpserts())
            .extracting(BulkWriteUpsert::getIndex)
            .containsExactly(0, 1);

        assertThat(collection.find())
            .containsExactly(json("_id: 1, a: 1"), json("_id: 2, a: 1"));

        models = List.of(
            new ReplaceOneModel<>(Filters.eq("_id", 1), json("_id: 1, a: 2"), new ReplaceOptions().upsert(true)),
            new ReplaceOneModel<>(Filters.eq("_id", 3), json("_id: 3, a: 2"), new ReplaceOptions().upsert(true)),
            new ReplaceOneModel<>(Filters.eq("_id", 2), json("_id: 2, a: 2"), new ReplaceOptions().upsert(true))
        );

        result = collection.bulkWrite(models, new BulkWriteOptions().ordered(false));
        assertThat(result.getUpserts())
            .extracting(BulkWriteUpsert::getId)
            .containsExactly(new BsonInt32(3));
        assertThat(result.getUpserts())
            .extracting(BulkWriteUpsert::getIndex)
            .containsExactly(1);

        assertThat(collection.find())
            .containsExactlyInAnyOrder(
                json("_id: 1, a: 2"),
                json("_id: 2, a: 2"),
                json("_id: 3, a: 2")
            );
    }

    @Test
    void testUpdateWithMultiplyOperator() {
        Document object = json("_id: 1");

        collection.insertOne(object);

        collection.updateOne(object, json("$mul: {a: 2}, $set: {b: 2}"));
        assertThat(collection.find(object).first()).isEqualTo(json("_id: 1, a: 0, b: 2"));

        collection.updateOne(object, json("$mul: {b: 2.5}, $inc: {a: 0.5}"));
        assertThat(collection.find(object).first()).isEqualTo(json("_id: 1, a: 0.5, b: 5.0"));
    }

    @Test
    void testUpdateWithIllegalMultiplyFails() {
        Document object = json("_id: 1, foo: 'x', bar: 1");

        collection.insertOne(object);

        assertMongoWriteException(() -> collection.updateOne(object, json("$mul: {_id: 2}")),
            66, "ImmutableField", "Performing an update on the path '_id' would modify the immutable field '_id'");

        assertMongoWriteException(() -> collection.updateOne(object, json("$mul: {foo: 2}")),
            14, "TypeMismatch", "Cannot apply $mul to a value of non-numeric type. {_id: 1} has the field 'foo' of non-numeric type string");

        assertMongoWriteException(() -> collection.updateOne(object, json("$mul: {bar: 'x'}")),
            14, "TypeMismatch", "Cannot multiply with non-numeric argument: {bar: \"x\"}");
    }

    @Test
    void testIsMaster() {
        Document isMaster = runCommand("isMaster");
        assertThat(isMaster.getBoolean("ismaster")).isTrue();
        assertThat(isMaster.getDate("localTime")).isInstanceOf(Date.class);
        Integer maxBsonObjectSize = isMaster.getInteger("maxBsonObjectSize");
        assertThat(maxBsonObjectSize).isEqualTo(16777216);
        assertThat(isMaster.getInteger("maxMessageSizeBytes")).isGreaterThan(maxBsonObjectSize);
    }

    // https://github.com/foursquare/fongo/pull/26
    // http://stackoverflow.com/questions/12403240/storing-null-vs-not-storing-the-key-at-all-in-mongodb
    @Test
    void testFindWithNullOrNoFieldFilter() {

        collection.insertOne(json("name: 'jon', group: 'group1'"));
        collection.insertOne(json("name: 'leo', group: 'group1'"));
        collection.insertOne(json("name: 'neil1', group: 'group2'"));
        collection.insertOne(json("name: 'neil2', group: null"));
        collection.insertOne(json("name: 'neil3'"));

        // check {group: null} vs {group: {$exists: false}} filter
        assertThat(collection.find(json("group: null")))
            .as("should have two neils (neil2, neil3)")
            .hasSize(2);

        assertThat(collection.find(exists("group", false)))
            .as("should have one neils (neil3)")
            .hasSize(1);

        // same check but for fields which do not exist in DB
        assertThat(collection.find(json("other: null")))
            .as("should return all documents")
            .hasSize(5);

        assertThat(collection.find(exists("other", false)))
            .as("should return all documents")
            .hasSize(5);
    }

    @Test
    void testInsertWithIllegalId() {
        assertMongoWriteException(() -> collection.insertOne(json("_id: [1, 2, 3]")),
            53, "InvalidIdField", "The '_id' value cannot be of type array");
    }

    @Test
    void testInsertsWithUniqueIndex() {
        collection.createIndex(new Document("uniqueKeyField", 1), new IndexOptions().unique(true));

        collection.insertOne(json("uniqueKeyField: 'abc1', afield: 'avalue'"));
        collection.insertOne(json("uniqueKeyField: 'abc2', afield: 'avalue'"));
        collection.insertOne(json("uniqueKeyField: 'abc3', afield: 'avalue'"));

        assertMongoWriteException(() -> collection.insertOne(json("uniqueKeyField: 'abc2', afield: 'avalue'")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: uniqueKeyField_1 dup key: { uniqueKeyField: \"abc2\" }");

        collection.insertOne(json("uniqueKeyField: 1"));
        collection.insertOne(json("uniqueKeyField: 1.1"));

        assertMongoWriteException(() -> collection.insertOne(json("uniqueKeyField: 1.0")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: uniqueKeyField_1 dup key: { uniqueKeyField: 1.0 }");
    }

    @Test
    void testInsertBinaryData() {
        collection.insertOne(new Document("test", new byte[] { 0x01, 0x02, 0x03 }));
    }

    // see https://github.com/bwaldvogel/mongo-java-server/issues/9
    @Test
    void testUniqueIndexWithSubdocument() {
        collection.createIndex(json("'action.actionId': 1"), new IndexOptions().unique(true));

        collection.insertOne(json("_id: 1, action: 'abc1'"));
        collection.insertOne(json("_id: 2, action: {actionId: 1}"));
        collection.insertOne(json("_id: 3, action: {actionId: 2}"));
        collection.insertOne(json("_id: 4, action: {actionId: 3}"));

        assertMongoWriteException(() -> collection.insertOne(json("action: {actionId: 1.0}")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: action.actionId_1 dup key: { action.actionId: 1.0 }");

        assertThat(collection.find(json("action: 'abc1'")))
            .containsExactly(json("_id: 1, action: 'abc1'"));

        assertThat(collection.find(json("'action.actionId': 2")))
            .containsExactly(json("_id: 3, action: {actionId: 2}"));

        assertThat(collection.find(json("action: {actionId: 2}")))
            .containsExactly(json("_id: 3, action: {actionId: 2}"));

        assertThat(collection.find(json("'action.actionId.subKey': 23"))).isEmpty();
    }

    @Test
    public void testUniqueIndexWithDeepDocuments() {
        collection.createIndex(json("a: 1"), new IndexOptions().unique(true));

        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2, a: 1"));
        collection.insertOne(json("_id: 3, a: {b: 0}"));
        collection.insertOne(json("_id: 4, a: {b: {c: 1}}"));
        collection.insertOne(json("_id: 5, a: {b: {c: 1, d: 1}}"));
        collection.insertOne(json("_id: 6, a: {b: {d: 1, c: 1}}"));
        collection.insertOne(json("_id: 7, a: {b: 1, c: 1}"));
        collection.insertOne(json("_id: 8, a: {c: 1, d: 1}"));
        collection.insertOne(json("_id: 9, a: {c: 1}"));

        assertMongoWriteException(() -> collection.insertOne(json("a: {b: 0}")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: a_1 dup key: { a: { b: 0 } }");

        assertMongoWriteException(() -> collection.insertOne(json("a: {b: 0.00}")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: a_1 dup key: { a: { b: 0.0 } }");

        assertMongoWriteException(() -> collection.insertOne(json("a: {b: -0.0}")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: a_1 dup key: { a: { b: -0.0 } }");

        assertMongoWriteException(() -> collection.insertOne(json("a: {b: {c: 1.0}}")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: a_1 dup key: { a: { b: { c: 1.0 } } }");

        assertMongoWriteException(() -> collection.insertOne(json("a: {b: {c: 1, d: 1.0}}")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: a_1 dup key: { a: { b: { c: 1, d: 1.0 } } }");
    }

    // see https://github.com/bwaldvogel/mongo-java-server/issues/39
    @Test
    void testSecondaryUniqueIndexUpdate() {
        collection.createIndex(json("text: 1"), new IndexOptions().unique(true));

        collection.insertOne(json("_id: 1, text: 'abc'"));
        collection.insertOne(json("_id: 2, text: 'def'"));
        collection.insertOne(json("_id: 3"));

        assertMongoWriteException(() -> collection.insertOne(json("_id: 4")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: text_1 dup key: { text: null }");

        assertMongoWriteException(() -> collection.updateOne(json("_id: 1"), new Document("$set", json("text: 'def'"))),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: text_1 dup key: { text: \"def\" }");

        collection.updateOne(json("_id: 1"), new Document("$set", json("text: 'xyz'")));
        collection.updateOne(json("_id: 2"), new Document("$set", json("text: 'abc'")));

        assertMongoWriteException(() -> collection.updateOne(json("_id: 2"), new Document("$set", json("text: null"))),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: text_1 dup key: { text: null }");

        collection.deleteOne(json("text: 'xyz'"));

        assertThat(collection.find())
            .containsExactlyInAnyOrder(
                json("_id: 2, text: 'abc'"),
                json("_id: 3")
            );
    }

    // see https://github.com/bwaldvogel/mongo-java-server/issues/39
    @Test
    public void testSecondarySparseUniqueIndex() {
        collection.createIndex(json("text: 1"), new IndexOptions().unique(true).sparse(true));

        collection.insertOne(json("_id: 1, text: 'abc'"));
        collection.insertOne(json("_id: 2, text: 'def'"));
        collection.insertOne(json("_id: 3"));
        collection.insertOne(json("_id: 4"));
        collection.insertOne(json("_id: 5, text: null"));

        assertMongoWriteException(() -> collection.insertOne(json("_id: 6, text: null")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: text_1 dup key: { text: null }");

        assertMongoWriteException(() -> collection.insertOne(json("_id: 7, text: 'abc'")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: text_1 dup key: { text: \"abc\" }");

        assertMongoWriteException(() -> collection.updateOne(json("_id: 2"), new Document("$set", json("text: null"))),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: text_1 dup key: { text: null }");

        collection.deleteOne(json("_id: 5"));

        collection.updateOne(json("_id: 2"), new Document("$set", json("text: null")));
        collection.updateOne(json("_id: 1"), new Document("$set", json("text: 'def'")));

        collection.deleteMany(json("text: null"));

        assertThat(collection.find()).containsExactly(json("_id: 1, text: 'def'"));
    }

    // see https://github.com/bwaldvogel/mongo-java-server/issues/39
    @Test
    public void testCompoundSparseUniqueIndex() {
        collection.createIndex(json("a: 1, b: 1"), new IndexOptions().unique(true).sparse(true));

        collection.insertOne(json("_id: 1, a: 10, b: 20"));
        collection.insertOne(json("_id: 2, a: 10"));
        collection.insertOne(json("_id: 3, b: 20"));
        collection.insertOne(json("_id: 4"));
        collection.insertOne(json("_id: 5"));
        collection.insertOne(json("_id: 6, a: null"));

        assertMongoWriteException(() -> collection.insertOne(json("_id: 7, a: null")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: a_1_b_1 dup key: { a: null, b: null }");

        assertMongoWriteException(() -> collection.insertOne(json("_id: 7, b: null")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: a_1_b_1 dup key: { a: null, b: null }");

        collection.deleteMany(json("a: null, b: null"));

        assertThat(collection.find())
            .containsExactlyInAnyOrder(
                json("_id: 1, a: 10, b: 20"),
                json("_id: 2, a: 10"),
                json("_id: 3, b: 20")
            );
    }

    @Test
    public void testCompoundSparseUniqueIndexOnEmbeddedDocuments() {
        collection.createIndex(json("'a.x': 1, 'b.x': 1"), new IndexOptions().unique(true).sparse(true));

        collection.insertOne(json("_id: 1, a: 10, b: 20"));
        collection.insertOne(json("_id: 2, a: 10"));
        collection.insertOne(json("_id: 3, b: 20"));
        collection.insertOne(json("_id: 4, a: {x: 1}"));
        collection.insertOne(json("_id: 5, b: {x: 2}"));
        collection.insertOne(json("_id: 6, a: {x: 1}, b: {x: 2}"));
        collection.insertOne(json("_id: 7, a: {x: 2}, b: {x: 2}"));
        collection.insertOne(json("_id: 8, a: {x: null}, b: {x: null}"));
        collection.insertOne(json("_id: 9"));

        assertMongoWriteException(() -> collection.insertOne(json("_id: 10, a: {x: 1.0}, b: {x: 2.0}")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: a.x_1_b.x_1 dup key: { a.x: 1.0, b.x: 2.0 }");

        assertMongoWriteException(() -> collection.insertOne(json("_id: 11, a: {x: null}, b: {x: null}")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: a.x_1_b.x_1 dup key: { a.x: null, b.x: null }");

        collection.deleteMany(json("a: {x: null}, b: {x: null}"));
        collection.deleteMany(json("a: 10"));
        collection.deleteMany(json("b: 20"));

        assertThat(collection.find())
            .containsExactlyInAnyOrder(
                json("_id: 4, a: {x: 1}"),
                json("_id: 5, b: {x: 2}"),
                json("_id: 6, a: {x: 1}, b: {x: 2}"),
                json("_id: 7, a: {x: 2}, b: {x: 2}"),
                json("_id: 9")
            );
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/90
    @Test
    void testUpdateWithSparseUniqueIndex() {
        collection.createIndex(json("a: 1"), new IndexOptions().unique(true).sparse(true));

        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));

        collection.updateOne(json("_id: 1"), json("$set: {a: 'x'}"));
        collection.updateOne(json("_id: 2"), json("$set: {a: 'y'}"));

        assertMongoWriteException(() -> collection.updateOne(json("_id: 1"), json("$set: {a: 'y'}")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: a_1 dup key: { a: \"y\" }");

        collection.updateOne(json("_id: 2"), json("$unset: {a: 1}"));
        collection.updateOne(json("_id: 1"), json("$set: {a: 'y'}"));
    }

    @Test
    public void testSparseUniqueIndexOnEmbeddedDocument() {
        collection.createIndex(json("'a.b.c': 1"), new IndexOptions().unique(true).sparse(true));

        collection.insertOne(json("a: 1"));
        collection.insertOne(json("a: 1"));
        collection.insertOne(json("a: null"));
        collection.insertOne(json("a: null"));
        collection.insertOne(json("a: {b: 1}"));
        collection.insertOne(json("a: {b: 1}"));
        collection.insertOne(json("a: {b: null}"));
        collection.insertOne(json("a: {b: null}"));
        collection.insertOne(json("a: {b: {c: 1}}"));
        collection.insertOne(json("a: {b: {c: 2}}"));
        collection.insertOne(json("a: {b: {c: null}}"));
        collection.insertOne(json("a: {b: {c: {d: 1}}}"));
        collection.insertOne(json("a: {b: {c: {d: null}}}"));

        assertMongoWriteException(() -> collection.insertOne(json("a: {b: {c: 1}}")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: a.b.c_1 dup key: { a.b.c: 1 }");

        assertMongoWriteException(() -> collection.insertOne(json("a: {b: {c: null}}")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: a.b.c_1 dup key: { a.b.c: null }");

        assertMongoWriteException(() -> collection.insertOne(json("a: {b: {c: 1, x: 100}}")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: a.b.c_1 dup key: { a.b.c: 1 }");

        assertMongoWriteException(() -> collection.insertOne(json("a: {b: {c: {d: 1}}}")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: a.b.c_1 dup key: { a.b.c: { d: 1 } }");

        assertMongoWriteException(() -> collection.insertOne(json("a: {b: {c: {d: null}}}")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: a.b.c_1 dup key: { a.b.c: { d: null } }");
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/201
    @Test
    public void testUniqueIndexOnArrayField() {
        collection.createIndex(json("a: 1"), new IndexOptions().unique(true));

        collection.insertOne(json("_id: 1, a: ['val1', 'val2']"));
        collection.insertOne(json("_id: 2, a: ['val3', 'val4']"));
        collection.insertOne(json("_id: 3, a: []"));
        collection.insertOne(json("_id: 4, a: ['val5']"));

        assertMongoWriteException(() -> collection.insertOne(json("a: ['val1']")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: a_1 dup key: { a: \"val1\" }'");

        assertThat(collection.find(json("a: ['val1']")))
            .isEmpty();

        assertThat(collection.find(json("a: ['val1', 'val3']")))
            .isEmpty();

        assertThat(collection.find(json("a: ['val1', 'val10']")))
            .isEmpty();

        assertThat(collection.find(json("a: ['val10']")))
            .isEmpty();

        assertThat(collection.find(json("a: ['val5']")))
            .containsExactly(json("_id: 4, a: ['val5']"));

        assertThat(collection.find(json("a: ['val1', 'val2']")))
            .containsExactly(json("_id: 1, a: ['val1', 'val2']"));

        assertThat(collection.find(json("a: ['val2', 'val1']")))
            .isEmpty();

        assertThat(collection.find(json("a: {$all: ['val1', 'val2']}")))
            .containsExactly(json("_id: 1, a: ['val1', 'val2']"));

        assertThat(collection.find(json("a: {$all: ['val2', 'val1']}")))
            .containsExactly(json("_id: 1, a: ['val1', 'val2']"));
    }

    @Test
    public void testUniqueIndexOnArrayField_updates() {
        collection.createIndex(json("a: 1"), new IndexOptions().unique(true));

        collection.insertOne(json("_id: 1, a: ['val1', 'val2']"));
        collection.insertOne(json("_id: 2, a: ['val3', 'val4']"));

        assertMongoWriteException(() -> collection.replaceOne(json("_id: 1"), json("_id: 1, a: ['val1', 'val3']")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: a_1 dup key: { a: \"val3\" }'");

        assertMongoWriteException(() -> collection.updateOne(json("_id: 1"), json("$push: {a: 'val3'}")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: a_1 dup key: { a: \"val3\" }");

        collection.replaceOne(json("_id: 1"), json("_id: 1, a: ['val1', 'val5']"));
        collection.insertOne(json("_id: 3, a: ['val2']"));

        assertThat(collection.find(json("a: ['val1', 'val5']")))
            .containsExactly(json("_id: 1, a: ['val1', 'val5']"));

        assertThat(collection.find(json("a: ['val2']")))
            .containsExactly(json("_id: 3, a: ['val2']"));

        collection.updateOne(json("a: ['val2']"), json("$push: {a: 'val7'}"));

        assertThat(collection.find(json("a: ['val2', 'val7']")))
            .containsExactly(json("_id: 3, a: ['val2', 'val7']"));
    }

    @Test
    public void testUniqueIndexOnArrayFieldInSubdocument() {
        collection.createIndex(json("'a.b': 1"), new IndexOptions().unique(true));

        collection.insertOne(json("_id: 1, a: {b: ['val1', 'val2']}"));
        collection.insertOne(json("_id: 2, a: {b: ['val3', 'val4']}"));
        collection.insertOne(json("_id: 3, a: []"));
        collection.insertOne(json("_id: 4, a: {b: 'val5'}"));

        assertMongoWriteException(() -> collection.insertOne(json("a: {b: ['val1']}")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: a.b_1 dup key: { a.b: \"val1\" }'");

        assertThat(collection.find(json("'a.b': 'val5'")))
            .containsExactly(json("_id: 4, a: {b: 'val5'}"));

        assertThat(collection.find(json("'a.b': ['val1', 'val2']")))
            .containsExactly(json("_id: 1, a: {b: ['val1', 'val2']}"));

        assertThat(collection.find(json("a: {b: ['val1', 'val2']}")))
            .containsExactly(json("_id: 1, a: {b: ['val1', 'val2']}"));

        assertThat(collection.find(json("'a.b': ['val1']")))
            .isEmpty();
    }

    @Test
    void testAddNonUniqueIndexOnNonIdField() {
        collection.insertOne(json("someField: 'abc'"));
        assertThat(collection.listIndexes()).hasSize(1);

        collection.createIndex(new Document("someField", 1), new IndexOptions().unique(false));
        assertThat(collection.listIndexes()).hasSize(2);

        collection.insertOne(json("someField: 'abc'"));
    }

    @Test
    void testAddSparseIndexOnNonIdField() {
        collection.insertOne(json("someField: 'abc'"));
        assertThat(collection.listIndexes()).hasSize(1);

        collection.createIndex(new Document("someField", 1), new IndexOptions().sparse(true));
        assertThat(collection.listIndexes()).hasSize(2);

        collection.insertOne(json("someField: 'abc'"));
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/61
    @Test
    void testDeleteAllDocumentsWithUniqueSparseIndex() {
        collection.createIndex(new Document("someField.values", 1), new IndexOptions().unique(true).sparse(true));

        collection.insertOne(json("_id: 1, someField: {values: ['abc']}"));
        collection.insertOne(json("_id: 2, someField: {values: ['other']}"));
        collection.insertOne(json("_id: 3"));
        collection.insertOne(json("_id: 4, someField: ['abc']"));
        collection.insertOne(json("_id: 5, someField: 'abc'"));
        collection.insertOne(json("_id: 6, someField: null"));

        collection.deleteMany(json(""));

        assertThat(collection.countDocuments()).isZero();
    }

    @Test
    void testAddPartialIndexOnNonIdField() {
        collection.insertOne(json("someField: 'abc'"));
        assertThat(collection.listIndexes()).hasSize(1);

        collection.createIndex(new Document("someField", 1), new IndexOptions()
            .partialFilterExpression(json("someField: {$gt: 5}")));

        assertThat(collection.listIndexes()).hasSize(2);

        collection.insertOne(json("someField: 'abc'"));
    }

    @Test
    void testCompoundUniqueIndices() {
        collection.createIndex(json("a: 1, b: 1"), new IndexOptions().unique(true));

        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2, a: 'foo'"));
        collection.insertOne(json("_id: 3, b: 'foo'"));
        collection.insertOne(json("_id: 4, a: 'foo', b: 'foo'"));
        collection.insertOne(json("_id: 5, a: 'foo', b: 'bar'"));
        collection.insertOne(json("_id: 6, a: 'bar', b: 'foo'"));
        collection.insertOne(json("_id: 7, a: {x: 1, y: 1}, b: 'foo'"));
        collection.insertOne(json("_id: 8, a: {x: 1, y: 2}, b: 'foo'"));

        assertMongoWriteException(() -> collection.insertOne(json("a: 'foo', b: 'foo'")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: a_1_b_1 dup key: { a: \"foo\", b: \"foo\" }");

        assertMongoWriteException(() -> collection.insertOne(json("b: 'foo'")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: a_1_b_1 dup key: { a: null, b: \"foo\" }");

        assertMongoWriteException(() -> collection.insertOne(json("a: {x: 1, y: 1}, b: 'foo'")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: a_1_b_1 dup key: { a: { x: 1, y: 1 }, b: \"foo\" }");

        assertThat(collection.find(json("a: 'bar'")))
            .containsExactly(json("_id: 6, a: 'bar', b: 'foo'"));

        assertThat(collection.find(json("b: 'foo', a: 'bar'")))
            .containsExactly(json("_id: 6, a: 'bar', b: 'foo'"));

        assertThat(collection.find(json("a: 'foo'")))
            .containsExactlyInAnyOrder(
                json("_id: 2, a: 'foo'"),
                json("_id: 4, a: 'foo', b: 'foo'"),
                json("_id: 5, a: 'foo', b: 'bar'")
            );
    }

    @Test
    void testCompoundUniqueIndices_Subdocument() {
        collection.createIndex(json("a: 1, 'b.c': 1"), new IndexOptions().unique(true));

        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2, a: 'foo', b: 'foo'"));
        collection.insertOne(json("_id: 3, a: 'bar', b: {c: 1}"));
        collection.insertOne(json("_id: 4, a: 'bar', b: {c: 2}"));

        assertMongoWriteException(() -> collection.insertOne(json("a: 'bar', b: {c: 1}")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: a_1_b.c_1 dup key: { a: \"bar\", b.c: 1 }");
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/80
    @Test
    void testCompoundUniqueIndicesWithInQuery() {
        collection.createIndex(json("a: 1, b: 1"), new IndexOptions().unique(true));

        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2, a: 'foo'"));
        collection.insertOne(json("_id: 3, b: 'foo'"));
        collection.insertOne(json("_id: 4, a: 'foo', b: 'foo'"));
        collection.insertOne(json("_id: 5, a: 'foo', b: 'bar'"));

        assertThat(collection.find(json("a: 'foo', b: { $in: ['bar'] }")))
            .hasSize(1);
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/83
    @Test
    void testAddUniqueIndexOnExistingDocuments() {
        collection.insertOne(json("_id: 1, value: 'a'"));
        collection.insertOne(json("_id: 2, value: 'b'"));
        collection.insertOne(json("_id: 3, value: 'c'"));

        collection.createIndex(json("value: 1"), new IndexOptions().unique(true));

        assertMongoWriteException(() -> collection.insertOne(json("value: 'c'")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: value_1 dup key: { value: \"c\" }");

        collection.insertOne(json("_id: 4, value: 'd'"));
    }

    @Test
    public void testAddUniqueIndexOnExistingDocuments_violatingUniqueness() {
        collection.insertOne(json("_id: 1, value: 'a'"));
        collection.insertOne(json("_id: 2, value: 'b'"));
        collection.insertOne(json("_id: 3, value: 'c'"));
        collection.insertOne(json("_id: 4, value: 'b'"));

        assertThatExceptionOfType(DuplicateKeyException.class)
            .isThrownBy(() -> collection.createIndex(json("value: 1"), new IndexOptions().unique(true)))
            .withMessageMatching("Write failed with error code 11000 and error message " +
                "'Index build failed: [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}: " +
                "Collection testdb\\.testcoll \\( [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12} \\) :: caused by :: " +
                "E11000 duplicate key error collection: testdb.testcoll index: value_1 dup key: \\{ value: \"b\" \\}'");

        assertThat(collection.listIndexes())
            .containsExactly(json("name: '_id_', key: {_id: 1}, v: 2"));

        collection.insertOne(json("_id: 5, value: 'a'"));
    }

    @Test
    void testCursorOptionNoTimeout() {
        try (MongoCursor<Document> cursor = collection.find().noCursorTimeout(true).iterator()) {
            assertThat(cursor.hasNext()).isFalse();
        }
    }

    @Test
    void testBulkInsert() {
        List<WriteModel<Document>> inserts = new ArrayList<>();
        inserts.add(new InsertOneModel<>(json("_id: 1")));
        inserts.add(new InsertOneModel<>(json("_id: 2")));
        inserts.add(new InsertOneModel<>(json("_id: 3")));

        BulkWriteResult result = collection.bulkWrite(inserts);
        assertThat(result.getInsertedCount()).isEqualTo(3);
    }

    @Test
    void testLargeBulkInsert() {
        List<WriteModel<Document>> inserts = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            inserts.add(new InsertOneModel<>(new Document("_id", i + 1)
                .append("data", "some longer string too cause some data on the wire")));
        }

        BulkWriteResult result = collection.bulkWrite(inserts);
        assertThat(result.getInsertedCount()).isEqualTo(1000);
    }

    @Test
    void testBulkInsert_withDuplicate() {
        collection.insertOne(json("_id: 2"));

        List<WriteModel<Document>> inserts = new ArrayList<>();
        inserts.add(new InsertOneModel<>(json("_id: 1")));
        inserts.add(new InsertOneModel<>(json("_id: 2")));
        inserts.add(new InsertOneModel<>(json("_id: 3")));

        assertThatExceptionOfType(MongoBulkWriteException.class)
            .isThrownBy(() -> collection.bulkWrite(inserts))
            .withMessageContaining("BulkWriteError{index=1, code=11000, message='E11000 duplicate key error collection: testdb.testcoll index: _id_ dup key: { _id: 2 }'");

        assertThat(collection.find().sort(json("_id: 1")))
            .containsExactly(
                json("_id: 1"),
                json("_id: 2")
            );
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/142
    @Test
    void testBulkInsert_unordered_withDuplicate() {
        List<WriteModel<Document>> inserts = new ArrayList<>();
        inserts.add(new InsertOneModel<>(json("_id: 1")));
        inserts.add(new InsertOneModel<>(json("_id: 2")));
        inserts.add(new InsertOneModel<>(json("_id: 2")));
        inserts.add(new InsertOneModel<>(json("_id: 3")));
        inserts.add(new InsertOneModel<>(json("_id: 3")));
        inserts.add(new InsertOneModel<>(json("_id: 4")));

        assertThatExceptionOfType(MongoBulkWriteException.class)
            .isThrownBy(() -> collection.bulkWrite(inserts, new BulkWriteOptions().ordered(false)))
            .withMessageContaining("Write errors: [" +
                "BulkWriteError{index=2, code=11000, message='E11000 duplicate key error collection: testdb.testcoll index: _id_ dup key: { _id: 2 }', details={}}, " +
                "BulkWriteError{index=4, code=11000, message='E11000 duplicate key error collection: testdb.testcoll index: _id_ dup key: { _id: 3 }', details={}}].");

        assertThat(collection.find().sort(json("_id: 1")))
            .containsExactly(
                json("_id: 1"),
                json("_id: 2"),
                json("_id: 3"),
                json("_id: 4")
            );
    }

    @Test
    void testBulkUpdateOrdered() {
        testBulkUpdate(true);
    }

    @Test
    void testBulkUpdateUnordered() {
        testBulkUpdate(false);
    }

    private void testBulkUpdate(boolean ordered) {
        insertUpdateInBulk(ordered);
        removeInBulk(ordered);
        insertUpdateInBulkNoMatch(ordered);
    }

    @Test
    void testUpdateCurrentDateIllegalTypeSpecification() {
        Document object = json("_id: 1");

        collection.insertOne(object);

        assertMongoWriteException(() -> collection.updateOne(object, json("$currentDate: {lastModified: null}")),
            2, "BadValue", "null is not valid type for $currentDate. Please use a boolean ('true') or a $type expression ({$type: 'timestamp/date'}).");

        assertMongoWriteException(() -> collection.updateOne(object, json("$currentDate: {lastModified: 123.456}")),
            2, "BadValue", "double is not valid type for $currentDate. Please use a boolean ('true') or a $type expression ({$type: 'timestamp/date'}).");

        assertMongoWriteException(() -> collection.updateOne(object, json("$currentDate: {lastModified: 'foo'}")),
            2, "BadValue", "string is not valid type for $currentDate. Please use a boolean ('true') or a $type expression ({$type: 'timestamp/date'}).");

        assertMongoWriteException(() -> collection.updateOne(object, json("$currentDate: {lastModified: {$type: 'foo'}}")),
            2, "BadValue", "The '$type' string field is required to be 'date' or 'timestamp': {$currentDate: {field : {$type: 'date'}}}");

        assertThat(collection.find(object).first()).isEqualTo(object);
    }

    @Test
    void testUpdateCurrentDate() {
        Document object = json("_id: 1");
        collection.insertOne(object);

        collection.updateOne(object, json("$currentDate: {'x.lastModified': true}"));
        assertThat(((Document) collection.find(object).first().get("x")).get("lastModified"))
            .isInstanceOf(Date.class);

        collection.updateOne(object, json("$currentDate: {'x.lastModified': {$type: 'date'}}"));
        assertThat(((Document) collection.find(object).first().get("x")).get("lastModified"))
            .isInstanceOf(Date.class);

        collection.updateOne(object, json("$currentDate: {'x.lastModified': {$type: 'timestamp'}}"));
        assertThat(((Document) collection.find(object).first().get("x")).get("lastModified"))
            .isInstanceOf(BsonTimestamp.class);
    }

    @Test
    void testRenameField() {
        Document object = json("_id: 1, foo: 'x', bar: 'y'");
        collection.insertOne(object);

        collection.updateOne(json("_id: 1"), json("$rename: {foo: 'foo2', bar: 'bar2'}"));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, foo2: 'x', bar2: 'y'"));

        collection.updateOne(json("_id: 1"), json("$rename: {'bar2': 'foo', foo2: 'bar'}"));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, bar: 'x', foo: 'y'"));

        collection.updateOne(json("_id: 1"), json("$rename: {'bar': 'bar2', 'missing': 'foo'}"));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, bar2: 'x', foo: 'y'"));
    }

    @Test
    void testRenameField_embeddedDocument() {
        Document object = json("_id: 1, foo: { a: 1, b: 2 }, bar: { c: 3, d: 4 }}");
        collection.insertOne(object);

        collection.updateOne(json("_id: 1"), json("$rename: {'foo.a': 'foo.z', 'bar.c': 'bar.x'}"));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, foo: { z: 1, b: 2 }, bar: { x: 3, d: 4 }}"));

        collection.updateOne(json("_id: 1"), json("$rename: {'foo.z': 'foo.a', 'bar.a': 'bar.b'}"));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, foo: { a: 1, b: 2 }, bar: { x: 3, d: 4 }}"));

        collection.updateOne(json("_id: 1"), json("$rename: {'missing.a': 'missing.b'}"));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, foo: { a: 1, b: 2 }, bar: { x: 3, d: 4 }}"));

        collection.updateOne(json("_id: 1"), json("$rename: {'foo.a': 'a', 'bar.x': 'bar.c'}"));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, foo: { b: 2 }, bar: { c: 3, d: 4 }, a: 1}"));

        assertThatExceptionOfType(MongoWriteException.class)
            .isThrownBy(() -> collection.updateOne(json("_id: 1"), json("$rename: {'foo.b.c': 'foo.b.d'}")));
    }

    @Test
    void testRenameFieldIllegalValue() {
        Document object = json("_id: 1, foo: 'x', bar: 'y'");
        collection.insertOne(object);

        assertMongoWriteException(() -> collection.updateOne(json("_id: 1"), json("$rename: {foo: 12345}")),
            2, "BadValue", "The 'to' field for $rename must be a string: foo: 12345");

        assertMongoWriteException(() -> collection.updateOne(json("_id: 1"), json("$rename: {'_id': 'id'}")),
            66, "ImmutableField", "Performing an update on the path '_id' would modify the immutable field '_id'");

        assertMongoWriteException(() -> collection.updateOne(json("_id: 1"), json("$rename: {foo: '_id'}")),
            66, "ImmutableField", "Performing an update on the path '_id' would modify the immutable field '_id'");

        assertMongoWriteException(() -> collection.updateOne(json("_id: 1"), json("$rename: {foo: 'bar', 'bar': 'bar2'}")),
            40, "ConflictingUpdateOperators", "Updating the path 'bar' would create a conflict at 'bar'");

        assertMongoWriteException(() -> collection.updateOne(json("_id: 1"), json("$rename: {bar: 'foo', bar2: 'foo'}")),
            40, "ConflictingUpdateOperators", "Updating the path 'foo' would create a conflict at 'foo'");
    }

    @Test
    void testRenameCollection() {
        collection.insertOne(json("_id: 1, a: 10"));
        collection.insertOne(json("_id: 2, a: 20"));
        collection.insertOne(json("_id: 3, a: 30"));

        collection.createIndex(new Document("a", 1), new IndexOptions().unique(true));

        collection.renameCollection(new MongoNamespace(collection.getNamespace().getDatabaseName(), "other-collection-name"));

        assertThat(db.listCollectionNames())
            .containsExactly("other-collection-name");

        MongoCollection<Document> otherCollection = getCollection("other-collection-name");
        assertThat(otherCollection.countDocuments()).isEqualTo(3);

        assertThat(otherCollection.listIndexes())
            .containsExactlyInAnyOrder(
                json("name: '_id_', key: {_id: 1}, v: 2"),
                json("name: 'a_1', key: {a: 1}, unique: true, v: 2")
            );

        assertThat(collection.listIndexes()).isEmpty();
    }

    @Test
    void testRenameCollection_targetAlreadyExists() {
        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));
        collection.insertOne(json("_id: 3"));

        MongoCollection<Document> otherCollection = db.getCollection("other-collection-name");
        otherCollection.insertOne(json("_id: 1"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.renameCollection(new MongoNamespace(db.getName(), "other-collection-name")))
            .withMessageStartingWith("Command execution failed on MongoDB server with error 48 (NamespaceExists): 'target namespace exists'");

        assertThat(db.listCollectionNames())
            .containsExactlyInAnyOrder(getCollectionName(), "other-collection-name");

        assertThat(collection.countDocuments()).isEqualTo(3);
        assertThat(getCollection("other-collection-name").countDocuments()).isEqualTo(1);
    }

    @Test
    void testRenameCollection_dropTarget() {
        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));
        collection.insertOne(json("_id: 3"));

        MongoCollection<Document> otherCollection = db.getCollection("other-collection-name");
        otherCollection.insertOne(json("_id: 1"));

        assertThat(collection.listIndexes()).extracting(index -> index.getString("name"))
            .containsExactly("_id_");

        collection.renameCollection(new MongoNamespace(db.getName(), "other-collection-name"),
            new RenameCollectionOptions().dropTarget(true));

        assertThat(db.listCollectionNames()).containsExactly("other-collection-name");

        MongoCollection<Document> renamedCollection = getCollection("other-collection-name");

        assertThat(renamedCollection.listIndexes()).extracting(index -> index.getString("name"))
            .containsExactly("_id_");

        assertThat(renamedCollection.countDocuments()).isEqualTo(3);
    }

    @Test
    void testListIndexes_empty() {
        assertThat(collection.listIndexes()).isEmpty();
    }

    @Test
    void testListIndexes() {
        collection.insertOne(json("_id: 1"));
        MongoCollection<Document> other = db.getCollection("other");
        other.insertOne(json("_id: 1"));

        collection.createIndex(json("bla: 1"));

        collection.createIndex(new Document("a", 1), new IndexOptions().unique(true));
        collection.createIndex(new Document("a", 1).append("b", -1.0), new IndexOptions().unique(true));

        assertThat(collection.listIndexes())
            .containsExactlyInAnyOrder(
                json("name: '_id_', key: {_id: 1}, v: 2"),
                json("name: 'bla_1', key: {bla: 1}, v: 2"),
                json("name: 'a_1', key: {a: 1}, unique: true, v: 2"),
                json("name: 'a_1_b_-1', key: {a: 1, b: -1.0}, unique: true, v: 2")
            );

        assertThat(other.listIndexes())
            .containsExactly(json("name: '_id_', key: {_id: 1}, v: 2"));
    }

    @Test
    void testFieldSelection_deselectId() {
        collection.insertOne(json("_id: 1, order:1, visits: 2"));

        Document document = collection.find(json("")).projection(json("_id: 0")).first();
        assertThat(document).isEqualTo(json("order:1, visits:2"));
    }

    @Test
    void testFieldSelection_deselectOneField() {
        Document obj = json("_id: 1, order:1, visits: 2, eid: 12345");
        collection.insertOne(obj);

        Document document = collection.find(new Document()).projection(json("visits: 0")).first();
        assertThat(document).isEqualTo(json("_id: 1, order:1, eid: 12345"));
    }

    @Test
    void testFieldSelection_deselectTwoFields() {
        Document obj = json("_id: 1, order:1, visits: 2, eid: 12345");
        collection.insertOne(obj);

        Document document = collection.find(new Document()).projection(json("visits: 0, eid: 0")).first();
        assertThat(document).isEqualTo(json("_id: 1, order:1"));
    }

    @Test
    void testFieldSelection_selectAndDeselectFields() {
        Document obj = json("_id: 1, order:1, visits: 2, eid: 12345");
        collection.insertOne(obj);

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(new Document()).projection(json("visits: 0, eid: 1")).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 31253 (Location31253): " +
                "'Cannot do inclusion on field eid in exclusion projection'");
    }

    @Test
    void testPullWithInPattern() {

        collection.insertOne(json("_id: 1, tags: ['aa', 'bb', 'ab', 'cc']"));

        collection.updateOne(json("_id: 1"), pullByFilter(in("tags", Pattern.compile("a+"))));

        assertThat(collection.find().first()).isEqualTo(json("_id: 1, tags: ['bb', 'cc']"));
    }

    @Test
    void testPullWithInPatternAnchored() {

        collection.insertOne(json("_id: 1, tags: ['aa', 'bb', 'ab', 'cc']"));

        collection.updateOne(json("_id: 1"), pullByFilter(in("tags", Pattern.compile("^a+$"))));

        assertThat(collection.find().first()).isEqualTo(json("_id: 1, tags: ['bb', 'ab', 'cc']"));
    }

    @Test
    void testPullWithInNumbers() {

        collection.insertOne(json("_id: 1, values: [1, 2, 2.5, 3.0, 4]"));

        collection.updateOne(json("_id: 1"), pullByFilter(in("values", List.of(2.0, 3, 4L))));

        assertThat(collection.find().first()).isEqualTo(json("_id: 1, values: [1, 2.5]"));
    }

    @Test
    void testDocumentWithHashMap() {
        Map<String, String> value = new HashMap<>();
        value.put("foo", "bar");

        collection.insertOne(new Document("_id", 1).append("map", value));
        Bson document = collection.find().first();
        assertThat(document).isEqualTo(json("_id: 1, map: {foo: 'bar'}"));
    }

    @Test
    void testFindAndOfOrs() {
        collection.insertOne(new Document("_id", 1).append("published", true).append("startDate", instant("2015-03-01T13:20:05Z")));
        collection.insertOne(new Document("_id", 2).append("published", true).append("expiration", instant("2020-12-31T18:00:00Z")));
        collection.insertOne(new Document("_id", 3).append("published", true));
        collection.insertOne(new Document("_id", 4).append("published", false));
        collection.insertOne(new Document("_id", 5).append("published", true).append("startDate", instant("2017-01-01T00:00:00Z")));
        collection.insertOne(new Document("_id", 6).append("published", true).append("expiration", instant("2016-01-01T00:00:00Z")));

        Instant instant = instant("2016-01-01T00:00:00Z");
        Bson query = and(
            ne("published", false),
            or(exists("startDate", false), lt("startDate", instant)),
            or(exists("expiration", false), gt("expiration", instant))
        );

        assertThat(collection.find(query).projection(json("_id: 1")))
            .containsExactly(
                json("_id: 1"),
                json("_id: 2"),
                json("_id: 3")
            );
    }

    @Test
    void testInOperatorWithNullValue() {
        collection.insertMany(List.of(
            json("_id: 1, a: 1"),
            json("_id: 2, a: 2"),
            json("_id: 3, a: 3"),
            json("_id: 4, a: 4"),
            json("_id: 5"))
        );

        Bson inQueryWithNull = in("a", 2, null, 3);
        assertThat(collection.find(inQueryWithNull).projection(json("_id: 1")))
            .containsExactly(
                json("_id: 2"),
                json("_id: 3"),
                json("_id: 5")
            );
    }

    @Test
    void testQueryWithReference() {
        collection.insertOne(json("_id: 1"));
        String collectionName = getCollectionName();
        collection.insertOne(new Document("_id", 2).append("ref", new DBRef(collectionName, 1)));
        collection.insertOne(new Document("_id", 3).append("ref", new DBRef(collectionName, 2)));

        Document doc = collection.find(new Document("ref", new DBRef(collectionName, 1))).projection(json("_id: 1")).first();
        assertThat(doc).isEqualTo(json("_id: 2"));
    }

    @Test
    void testQueryWithIllegalReference() {
        collection.insertOne(json("_id: 1"));
        String collectionName = getCollectionName();
        collection.insertOne(new Document("_id", 2).append("ref", new DBRef(collectionName, 1)));
        collection.insertOne(new Document("_id", 3).append("ref", new DBRef(collectionName, 2)));

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(json("ref: {$ref: 'coll'}")).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 2 (BadValue): 'unknown operator: $ref'");
    }

    @Test
    void testAndOrNorWithEmptyArray() {
        collection.insertOne(json(""));

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(and()).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 2 (BadValue): '$and/$or/$nor must be a nonempty array'");

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(nor()).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 2 (BadValue): '$and/$or/$nor must be a nonempty array'");

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(or()).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 2 (BadValue): '$and/$or/$nor must be a nonempty array'");
    }

    @Test
    void testInsertLargeDocument() {
        insertAndFindLargeDocument(100, 1);
        insertAndFindLargeDocument(1000, 2);
        insertAndFindLargeDocument(10000, 3);
    }

    @Test
    void testInsertAndUpdateAsynchronously() throws Exception {
        int numDocuments = 1000;
        CountDownLatch latch = new CountDownLatch(numDocuments);
        Queue<RuntimeException> errors = new LinkedBlockingQueue<>();
        Semaphore concurrentOperationsOnTheFly = new Semaphore(50); // prevent MongoWaitQueueFullException

        for (int i = 1; i <= numDocuments; i++) {
            final Document document = new Document("_id", i);
            for (int j = 0; j < 10; j++) {
                document.append("key-" + i + "-" + j, "value-" + i + "-" + j);
            }
            concurrentOperationsOnTheFly.acquire();
            asyncCollection.insertOne(document).subscribe(new Subscriber<>() {
                @Override
                public void onSubscribe(Subscription s) {
                    s.request(Integer.MAX_VALUE);
                }

                @Override
                public void onNext(InsertOneResult result) {
                    log.info("inserted {}", document);
                    Document query = new Document("_id", document.getInteger("_id"));
                    asyncCollection.updateOne(query, Updates.set("updated", true)).subscribe(new Subscriber<>() {
                        @Override
                        public void onSubscribe(Subscription s) {
                            s.request(Integer.MAX_VALUE);
                        }

                        @Override
                        public void onNext(UpdateResult updateResult) {
                            log.info("updated {}: {}", query, updateResult);
                        }

                        @Override
                        public void onError(Throwable t) {
                            handleError("update", t);
                        }

                        @Override
                        public void onComplete() {
                            release();
                        }
                    });
                }

                @Override
                public void onError(Throwable t) {
                    handleError("insert", t);
                }

                @Override
                public void onComplete() {
                    log.info("insert completed");
                }

                private void handleError(String operation, Throwable t) {
                    log.error(operation + " of {} failed", document, t);
                    RuntimeException exception = new RuntimeException("Failed to " + operation + " " + document, t);
                    errors.add(exception);
                    release();
                    throw exception;
                }

                private void release() {
                    latch.countDown();
                    concurrentOperationsOnTheFly.release();
                }
            });
        }

        boolean success = latch.await(30, TimeUnit.SECONDS);
        assertThat(success).isTrue();

        if (!errors.isEmpty()) {
            throw errors.poll();
        }

        log.info("finished");

        for (int i = 1; i <= numDocuments; i++) {
            Document query = new Document("_id", i);
            Document document = collection.find(query).first();
            assertThat(document).describedAs(query.toJson()).isNotNull();
            assertThat(document.getBoolean("updated")).describedAs(document.toJson()).isTrue();
        }

        long count = collection.countDocuments();
        assertThat(count).isEqualTo(numDocuments);
    }

    @Test
    void testAllQuery() {
        // see https://docs.mongodb.com/manual/reference/operator/query/all/
        collection.insertOne(new Document("_id", new ObjectId("5234cc89687ea597eabee675"))
            .append("code", "xyz")
            .append("tags", List.of("school", "book", "bag", "headphone", "appliance"))
            .append("qty", List.of(
                new Document().append("size", "S").append("num", 10).append("color", "blue"),
                new Document().append("size", "M").append("num", 45).append("color", "blue"),
                new Document().append("size", "L").append("num", 100).append("color", "green")
            )));

        collection.insertOne(new Document("_id", new ObjectId("5234cc8a687ea597eabee676"))
            .append("code", "abc")
            .append("tags", List.of("appliance", "school", "book"))
            .append("qty", List.of(
                new Document().append("size", "6").append("num", 100).append("color", "green"),
                new Document().append("size", "6").append("num", 50).append("color", "blue"),
                new Document().append("size", "8").append("num", 100).append("color", "brown")
            )));

        collection.insertOne(new Document("_id", new ObjectId("5234ccb7687ea597eabee677"))
            .append("code", "efg")
            .append("tags", List.of("school", "book"))
            .append("qty", List.of(
                new Document().append("size", "S").append("num", 10).append("color", "blue"),
                new Document().append("size", "M").append("num", 100).append("color", "blue"),
                new Document().append("size", "L").append("num", 100).append("color", "green")
            )));

        collection.insertOne(new Document("_id", new ObjectId("52350353b2eff1353b349de9"))
            .append("code", "ijk")
            .append("tags", List.of("electronics", "school"))
            .append("qty", List.of(
                new Document().append("size", "M").append("num", 100).append("color", "green")
            )));

        assertThat(collection.find(json("tags: {$all: ['appliance', 'school', 'book']}")))
            .extracting(d -> d.get("_id"))
            .containsExactly(new ObjectId("5234cc89687ea597eabee675"), new ObjectId("5234cc8a687ea597eabee676"));
    }

    @Test
    void testMatchesElementQuery() {
        collection.insertOne(json("_id: 1, results: [82, 85, 88]"));
        collection.insertOne(json("_id: 2, results: [75, 88, 89]"));

        assertThat(collection.find(json("results: {$elemMatch: {$gte: 80, $lt: 85}}")))
            .containsExactly(json("_id: 1, results: [82, 85, 88]"));
    }

    @Test
    void testMatchesElementInEmbeddedDocuments() {
        collection.insertOne(json("_id: 1, results: [{product: 'abc', score: 10}, {product: 'xyz', score: 5}]"));
        collection.insertOne(json("_id: 2, results: [{product: 'abc', score:  9}, {product: 'xyz', score: 7}]"));
        collection.insertOne(json("_id: 3, results: [{product: 'abc', score:  7}, {product: 'xyz', score: 8}]"));

        assertThat(collection.find(json("results: {$elemMatch: {product: 'xyz', score: {$gte: 8}}}")))
            .containsExactlyInAnyOrder(
                json("_id: 3, results: [{product: 'abc', score:  7}, {product: 'xyz', score: 8}]")
            );

        assertThat(collection.find(json("results: {$elemMatch: {product: 'xyz'}}}")))
            .containsExactlyInAnyOrder(
                json("_id: 1, results: [{product: 'abc', score: 10}, {product: 'xyz', score: 5}]"),
                json("_id: 2, results: [{product: 'abc', score:  9}, {product: 'xyz', score: 7}]"),
                json("_id: 3, results: [{product: 'abc', score:  7}, {product: 'xyz', score: 8}]")
            );
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/42
    @Test
    void testElemMatchWithExpression() {
        collection.insertOne(json("_id: 1, languages: [{key: 'C'}, {key: 'Java'}]"));
        collection.insertOne(json("_id: 2, languages: [{key: 'Python'}]"));
        collection.insertOne(json("_id: 3, languages: [{key: 'C++'}, {key: 'C'}]"));

        assertThat(collection.find(json("languages: {$elemMatch: {$or: [{key: 'C'}, {key: 'C++'}]}}")))
            .containsExactly(
                json("_id: 1, languages: [{key: 'C'}, {key: 'Java'}]"),
                json("_id: 3, languages: [{key: 'C++'}, {key: 'C'}]")
            );

        assertThat(collection.find(json("languages: {$elemMatch: {$and: [{key: 'Java'}, {key: {$ne: 'Python'}}]}}")))
            .containsExactly(
                json("_id: 1, languages: [{key: 'C'}, {key: 'Java'}]")
            );

        assertThat(collection.find(json("languages: {$elemMatch: {$nor: [{key: 'C'}, {key: 'C++'}, {key: 'Java'}]}}")))
            .containsExactly(
                json("_id: 2, languages: [{key: 'Python'}]")
            );
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/97
    @Test
    void testElemMatchAndAllQuery() {
        collection.insertOne(json("_id: 1, list: [{aa: 'bb'}, {cc: 'dd'}]"));
        collection.insertOne(json("_id: 2, list: [{aa: 'bb'}, {cc: 'ee'}]"));
        collection.insertOne(json("_id: 3, list: [{cc: 'dd'}]"));
        collection.insertOne(json("_id: 4"));
        collection.insertOne(json("_id: 5, list: []"));
        collection.insertOne(json("_id: 6, list: [{aa: 'bb'}, {cc: 'dd'}, {ee: 'ff'}]"));
        collection.insertOne(json("_id: 7, list: {aa: 'bb'}"));

        assertThat(collection.find(json("list: {$all: [{$elemMatch: {aa: 'bb'}}, {$elemMatch: {cc: 'dd'}}], $size: 2}")))
            .containsExactly(json("_id: 1, list: [{aa: 'bb'}, {cc: 'dd'}]"));

        assertThat(collection.find(json("list: {$size: 2, $all: [{$elemMatch: {aa: 'bb'}}, {$elemMatch: {cc: 'dd'}}]}")))
            .containsExactly(json("_id: 1, list: [{aa: 'bb'}, {cc: 'dd'}]"));

        assertThat(collection.find(json("list: {$all: [{$elemMatch: {aa: 'bb'}}], $size: 2}")))
            .containsExactly(
                json("_id: 1, list: [{aa: 'bb'}, {cc: 'dd'}]"),
                json("_id: 2, list: [{aa: 'bb'}, {cc: 'ee'}]")
            );

        assertThat(collection.find(json("list: {$all: [{$elemMatch: {$and: [{aa: {$ne: 'bb'}}, {cc: {$ne: 'dd'}}]}}]}")))
            .containsExactly(
                json("_id: 2, list: [{aa: 'bb'}, {cc: 'ee'}]"),
                json("_id: 6, list: [{aa: 'bb'}, {cc: 'dd'}, {ee: 'ff'}]")
            );
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/51
    @Test
    void testQueryWithElemMatch() {
        collection.insertOne(json("_id: 1, materials: [{materialId: 'A'}, {materialId: 'B'}, {materialId: 'C'}]"));
        collection.insertOne(json("_id: 2, materials: [{materialId: 'B'}]"));
        collection.insertOne(json("_id: 3, materials: []"));
        collection.insertOne(json("_id: 4"));
        collection.insertOne(json("_id: 5, materials: 'ABC'"));
        collection.insertOne(json("_id: 6, materials: {materialId: 'A'}"));

        assertThat(collection.find(json("materials: {$elemMatch: {materialId: 'A'}}")))
            .containsExactly(json("_id: 1, materials: [{materialId: 'A'}, {materialId: 'B'}, {materialId: 'C'}]"));
    }

    @Test
    void testProjectionWithElemMatch() {
        collection.insertOne(json("_id: 1, zipcode: 63109, students: [{name: 'john'}, {name: 'jess'}, {name: 'jeff'}]"));
        collection.insertOne(json("_id: 2, zipcode: 63110, students: [{name: 'ajax'}, {name: 'achilles'}]"));
        collection.insertOne(json("_id: 3, zipcode: 63109, students: [{name: 'ajax'}, {name: 'achilles'}]"));
        collection.insertOne(json("_id: 4, zipcode: 63109, students: [{name: 'barney'}]"));
        collection.insertOne(json("_id: 5, zipcode: 63109, students: [1, 2, 3]"));
        collection.insertOne(json("_id: 6, zipcode: 63109, students: {name: 'achilles'}"));

        Document query = json("zipcode: 63109");
        Document projection = json("students: {$elemMatch: {name: 'achilles'}}");

        assertThat(collection.find(query).projection(projection))
            .containsExactlyInAnyOrder(
                json("_id: 1"),
                json("_id: 3, students: [{name: 'achilles'}]"),
                json("_id: 4"),
                json("_id: 5"),
                json("_id: 6")
            );
    }

    @Test
    void testProjectionWithElemMatch_BigSubdocument() {
        collection.insertOne(json("_id: 1, zipcode: 63109," +
            " students: [" +
            "              {name: 'john', school: 102, age: 10}," +
            "              {name: 'jess', school: 102, age: 11}," +
            "              {name: 'jeff', school: 108, age: 15}" +
            "           ]"));

        collection.insertOne(json("_id: 2, zipcode: 63110," +
            " students: [" +
            "              {name: 'ajax', school: 100, age: 7}," +
            "              {name: 'achilles', school: 100, age: 8 }" +
            "           ]"));

        collection.insertOne(json("_id: 3, zipcode: 63109," +
            " students: [" +
            "              {name: 'ajax', school: 100, age: 7}," +
            "              {name: 'achilles', school: 100, age: 8}" +
            "           ]"));

        collection.insertOne(json("_id: 4, zipcode: 63109," +
            " students: [" +
            "              {name: 'barney', school: 102, age: 7}" +
            "           ]"));

        assertThat(collection.find(json("zipcode: 63109")).projection(json("students: {$elemMatch: {school: 102}}}")))
            .containsExactlyInAnyOrder(
                json("_id: 1, students: [{name: 'john', school: 102, age: 10}]"),
                json("_id: 3"),
                json("_id: 4, students: [{name: 'barney', school: 102, age: 7}]")
            );
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/104
    @Test
    void testQueryWithProjection_elemMatchAndPositionalOperator() {
        collection.insertOne(json("_id: 1, states: [{state: 'A', key: 'abc'}, {state: 'B', key: 'efg'}]"));
        collection.insertOne(json("_id: 2, states: [{state: 'B', key: 'abc'}, {state: 'B', key: 'efg'}]"));

        assertThat(collection.find(json("states: {$elemMatch: {state: {$eq: 'A'}, key: {$eq: 'abc'}}}")).
            projection(json("'states.$': 1")))
            .containsExactly(json("_id: 1, states: [{state: 'A', key: 'abc'}]"));
    }

    @Test
    void testProjectionWithExclusion() {
        collection.insertOne(json("_id: 1, states: [{state: 'A', key: 'abc'}, {state: 'B', key: 'efg'}]"));
        collection.insertOne(json("_id: 2, states: [{state: 'B', key: 'abc'}, {state: 'B', key: 'efg'}]"));

        assertThat(collection.find().
            projection(json("{'states.key': 0}")))
            .containsExactly(
                json("_id: 1, states: [{state: 'A'}, {state: 'B'}]"),
                json("_id: 2, states: [{state: 'B'}, {state: 'B'}]")
            );

        //And check we didn't mess up any data in the process!
        assertThat(collection.find())
            .containsExactly(
                json("_id: 1, states: [{state: 'A', key: 'abc'}, {state: 'B', key: 'efg'}]"),
                json("_id: 2, states: [{state: 'B', key: 'abc'}, {state: 'B', key: 'efg'}]")
            );

    }

    @Test
    void testProjectionWithSlice() {
        collection.insertOne(json("_id: 1, values: ['a', 'b', 'c', 'd', 'e']"));
        collection.insertOne(json("_id: 2, values: 'xyz'"));

        assertThat(collection.find(json("_id: 1")).projection(json("values: {$slice: 1}")))
            .containsExactly(json("_id: 1, values: ['a']"));

        assertThat(collection.find(json("_id: 1")).projection(json("values: {$slice: [0, 2]}")))
            .containsExactly(json("_id: 1, values: ['a', 'b']"));

        assertThat(collection.find(json("_id: 1")).projection(json("values: {$slice: [-3, 2]}")))
            .containsExactly(json("_id: 1, values: ['c', 'd']"));

        assertThat(collection.find(json("_id: 2")).projection(json("values: {$slice: 1}")))
            .containsExactly(json("_id: 2, values: 'xyz'"));

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(json("_id: 1")).projection(json("values: {$slice: ['a', 'b']}")).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 28724 (Location28724): " +
                "'First argument to $slice must be an array, but is of type: string'");

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(json("_id: 1")).projection(json("values: {$slice: ['xyz', 2]}")).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 28724 (Location28724): " +
                "'First argument to $slice must be an array, but is of type: string'");

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(json("_id: 1")).projection(json("values: {$slice: [1, 0]}")).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 28724 (Location28724): " +
                "'First argument to $slice must be an array, but is of type: int'");

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(json("_id: 1")).projection(json("values: {$slice: [1, 'xyz']}")).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 28724 (Location28724): " +
                "'First argument to $slice must be an array, but is of type: int'");

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(json("_id: 1")).projection(json("values: {$slice: [1, 2, 3]}")).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 28724 (Location28724): " +
                "'First argument to $slice must be an array, but is of type: int'");

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(json("_id: 1")).projection(json("values: {$slice: 'abc'}")).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 28667 (Location28667): " +
                "'Invalid $slice syntax. The given syntax { $slice: \"abc\" } did not match the find() syntax because :: " +
                "Location31273: $slice only supports numbers and [skip, limit] arrays :: " +
                "The given syntax did not match the expression $slice syntax. :: caused by :: " +
                "Expression $slice takes at least 2 arguments, and at most 3, but 1 were passed in.'");
    }

    @Test
    void testMatchesNullOrMissing() {
        collection.insertOne(json("_id: 1, x: null"));
        collection.insertOne(json("_id: 2"));
        collection.insertOne(json("_id: 3, x: 123"));

        assertThat(collection.find(json("x: null")))
            .containsExactlyInAnyOrder(
                json("_id: 1, x: null"),
                json("_id: 2")
            );
    }

    @Test
    void testIllegalElementMatchQuery() {
        collection.insertOne(json("_id: 1, results: [ 82, 85, 88 ]"));

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(json("results: {$elemMatch: [ 85 ]}")).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 2 (BadValue): '$elemMatch needs an Object'");

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(json("results: {$elemMatch: 1}")).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 2 (BadValue): '$elemMatch needs an Object'");
    }

    @Test
    void testQueryWithOperatorAndWithoutOperator() {
        collection.insertOne(json("_id: 1, x: {y: 23}"));
        collection.insertOne(json("_id: 2, x: 9"));
        collection.insertOne(json("_id: 3, x: 100"));

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(json("x: {$lt: 10, y: 23}")).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 2 (BadValue): 'unknown operator: y'");

        assertThat(collection.find(json("x: {y: 23, $lt: 10}"))).isEmpty();
        assertThat(collection.find(json("x: {y: {$lt: 100, z: 23}}"))).isEmpty();
        assertThat(collection.find(json("a: 123, x: {y: {$lt: 100, z: 23}}"))).isEmpty();
    }

    @Test
    void testQueryWithComment() {
        collection.insertOne(json("_id: 1, x: 2"));
        collection.insertOne(json("_id: 2, x: 3"));
        collection.insertOne(json("_id: 3, x: 4"));

        assertThat(collection.find(json("x: {$mod: [2, 0 ]}, $comment: 'Find even values.'")))
            .extracting(d -> d.get("_id"))
            .containsExactly(1, 3);
    }

    @Test
    void testValidate() {
        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> db.runCommand(new Document("validate", getCollectionName())))
            .withMessageStartingWith("Command execution failed on MongoDB server with error 26 (NamespaceNotFound): " +
                "'Collection 'testdb.testcoll' does not exist to validate.'");

        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));
        collection.insertOne(json("_id: 3"));

        collection.deleteOne(json("_id: 2"));

        Document result = db.runCommand(new Document("validate", getCollectionName()));
        assertThat(result.get("nrecords")).isEqualTo(2);
    }

    @Test
    void testGetLastError() {
        collection.insertOne(json("_id: 1"));

        Document actual = db.runCommand(json("getlasterror: 1"));
        assertThat(actual.get("n")).isEqualTo(0);
        assertThat(actual).containsKey("err");
        assertThat(actual.get("err")).isNull();
        assertThat(actual.get("ok")).isEqualTo(1.0);

        assertThatExceptionOfType(MongoWriteException.class)
            .isThrownBy(() -> collection.insertOne(json("_id: 1.0")))
            .withMessageContaining("E11000 duplicate key error collection: testdb.testcoll index: _id_ dup key: { _id: 1.0 }");

        Document lastError = db.runCommand(json("getlasterror: 1"));
        assertThat(lastError.get("code")).isEqualTo(11000);
        assertThat(lastError.getString("err")).contains("duplicate key");
        assertThat(lastError.getString("codeName")).isEqualTo("DuplicateKey");
        assertThat(lastError.get("ok")).isEqualTo(1.0);
    }

    @Test
    public void testResetError() {
        collection.insertOne(json("_id: 1"));

        assertThatExceptionOfType(MongoWriteException.class)
            .isThrownBy(() -> collection.insertOne(json("_id: 1.0")))
            .withMessageContaining("duplicate key error collection: testdb.testcoll index: _id_ dup key: { _id: 1.0 }");

        assertThat(db.runCommand(json("reseterror: 1")))
            .isEqualTo(json("ok: 1.0"));

        assertThat(db.runCommand(json("getlasterror: 1")))
            .containsAllEntriesOf(json("err: null, n: 0, ok: 1.0"));
    }

    @Test
    void testIllegalTopLevelOperator() {
        Document query = json("$illegalOperator: 1");

        collection.insertOne(json("_id: 1"));

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(query).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 2 (BadValue): " +
                "'unknown top level operator: $illegalOperator. " +
                "If you have a field name that starts with a '$' symbol, consider using $getField or $setField.'");
    }

    @Test
    void testExprQuery() {
        Document query = json("$expr: {$gt: ['$spent', '$budget']}");

        assertThat(collection.find(query)).isEmpty();

        collection.insertOne(json("_id: 1, category: 'food', budget: 400, spent: 450"));
        collection.insertOne(json("_id: 2, category: 'drinks', budget: 100, spent: 150"));
        collection.insertOne(json("_id: 3, category: 'clothes', budget: 100, spent: 50"));
        collection.insertOne(json("_id: 4, category: 'misc', budget: 500, spent: 300"));
        collection.insertOne(json("_id: 5, category: 'travel', budget: 200, spent: 650"));

        assertThat(collection.find(query))
            .containsExactly(
                json("_id: 1, category: 'food', budget: 400, spent: 450"),
                json("_id: 2, category: 'drinks', budget: 100, spent: 150"),
                json("_id: 5, category: 'travel', budget: 200, spent: 650")
            );

        assertThat(collection.find(json("_id: {$gt: 3}"))).hasSize(2);
        assertThat(collection.find(json("_id: {$gt: {$expr: {$literal: 3}}}"))).isEmpty();

        assertThat(collection.find(json("$expr: {$eq: ['$budget', {$multiply: ['$spent', 2]}]}")))
            .containsExactly(json("_id: 3, category: 'clothes', budget: 100, spent: 50"));
    }

    @Test
    void testExprQuery_IllegalFieldPath() {
        collection.insertOne(json("_id: 1"));

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(json("$expr: {$eq: ['$a.', 10]}")).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 40353 (Location40353): 'FieldPath must not end with a '.'.'");

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(json("$expr: {$eq: ['$.a', 10]}")).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 15998 (Location15998): 'FieldPath field names may not be empty strings.'");

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(json("$expr: {$eq: ['$a..1', 10]}")).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 15998 (Location15998): 'FieldPath field names may not be empty strings.'");
    }

    @Test
    void testQueryEmbeddedDocument() {
        collection.insertOne(json("_id: 1, b: null"));
        collection.insertOne(json("_id: 2, b: {c: null}"));
        collection.insertOne(json("_id: 3, b: {c: 123}"));
        collection.insertOne(json("_id: 4, b: {c: ['a', null, 'b']}"));
        collection.insertOne(json("_id: 5, b: {c: [1, 2, 3]}"));
        collection.insertOne(json("_id: 6"));
        collection.insertOne(json("_id: 7, b: {c: 1, d: 2}"));
        collection.insertOne(json("_id: 8, b: {c: {d: 1, e: 2}}"));

        assertThat(collection.find(json("'b.c': 1")))
            .containsExactlyInAnyOrder(
                json("_id: 5, b: {c: [1, 2, 3]}"),
                json("_id: 7, b: {c: 1, d: 2}")
            );

        assertThat(collection.find(json("b: {c: 1}"))).isEmpty();

        assertThat(collection.find(json("'b.c': null")))
            .containsExactlyInAnyOrder(
                json("_id: 1, b: null"),
                json("_id: 2, b: {c: null}"),
                json("_id: 4, b: {c: ['a', null, 'b']}"),
                json("_id: 6")
            );

        assertThat(collection.find(json("b: {c: null}")))
            .containsExactly(json("_id: 2, b: {c: null}"));

        assertThat(collection.find(json("'b.c': {d: 1}"))).isEmpty();

        assertThat(collection.find(json("'b.c': {d: {$gte: 1}}"))).isEmpty();
        assertThat(collection.find(json("'b.c': {d: {$gte: 1}, e: {$lte: 2}}"))).isEmpty();

        assertThat(collection.find(json("'b.c.d': {$gte: 1}")))
            .containsExactly(json("_id: 8, b: {c: {d: 1, e: 2}}"));

        assertThat(collection.find(json("'b.c': {d: 1, e: 2}")))
            .containsExactly(json("_id: 8, b: {c: {d: 1, e: 2}}"));

        assertThat(collection.find(json("'b.c.e': 2")))
            .containsExactly(json("_id: 8, b: {c: {d: 1, e: 2}}"));
    }

    @Test
    void testQueryWithEquivalentEmbeddedDocument() {
        collection.insertOne(json("_id:  1, a: {b: 1, c: 0}"));
        collection.insertOne(json("_id:  2, a: {b: 1, c: 0.0}"));
        collection.insertOne(json("_id:  3, a: {b: 1.0, c: 0.0}"));
        collection.insertOne(json("_id:  4, a: {b: 1.0, c: 0}"));
        collection.insertOne(json("_id:  5, a: {b: {c: 1.0}}"));
        collection.insertOne(json("_id:  6, a: {b: {c: 1}}"));
        collection.insertOne(json("_id:  7, a: {b: {c: 1, d: 1.0}}"));
        collection.insertOne(json("_id:  8, a: {c: 0, b: 1.0}"));
        collection.insertOne(json("_id:  9, a: {c: 0}"));
        collection.insertOne(json("_id: 10, a: {b: 1}"));

        assertThat(collection.find(json("a: {b: 1.0, c: -0.0}")))
            .containsExactlyInAnyOrder(
                json("_id: 1, a: {b: 1, c: 0}"),
                json("_id: 2, a: {b: 1, c: 0.0}"),
                json("_id: 3, a: {b: 1.0, c: 0.0}"),
                json("_id: 4, a: {b: 1.0, c: 0}")
            );

        assertThat(collection.find(json("a: {b: {c: 1}}")))
            .containsExactlyInAnyOrder(
                json("_id: 5, a: {b: {c: 1.0}}"),
                json("_id: 6, a: {b: {c: 1}}")
            );
    }

    @Test
    public void testOrderByMissingAndNull() {
        collection.insertOne(json("_id:  1, a: null"));
        collection.insertOne(json("_id:  2"));
        collection.insertOne(json("_id:  3, a: {b: 1}"));
        collection.insertOne(json("_id:  4, a: null"));
        collection.insertOne(json("_id:  5"));

        assertThat(collection.find(json("")).sort(json("a: 1, _id: 1")))
            .containsExactly(
                json("_id: 1, a: null"),
                json("_id: 2"),
                json("_id: 4, a: null"),
                json("_id: 5"),
                json("_id: 3, a: {b: 1}")
            );
    }

    @Test
    public void testOrderByEmbeddedDocument() {
        collection.insertOne(json("_id:  1, a: {b: 1, c: 0}"));
        collection.insertOne(json("_id:  2, a: {b: 1, c: 0.0}"));
        collection.insertOne(json("_id:  3, a: {b: 1, c: null}"));
        collection.insertOne(json("_id:  4, a: {b: 1.0, c: 0}"));
        collection.insertOne(json("_id:  5, a: {b: 1.0, c: 0.0}"));
        collection.insertOne(json("_id:  6, a: {b: 1.0, c: 0}"));
        collection.insertOne(json("_id:  7, a: {b: {c: 1.0}}"));
        collection.insertOne(json("_id:  8, a: {c: 0, b: 1.0}"));
        collection.insertOne(json("_id:  9, a: {c: 0}"));
        collection.insertOne(json("_id: 10, a: {b: 1}"));
        collection.insertOne(json("_id: 11, a: {b: {c: 0.0}}"));
        collection.insertOne(json("_id: 12, a: {c: 2}"));
        collection.insertOne(json("_id: 13, a: {b: null, c: 0}"));
        collection.insertOne(json("_id: 14, a: {b: 'abc'}"));
        collection.insertOne(json("_id: 15, a: null, b: 123"));
        collection.insertOne(json("_id: 16, b: 123"));
        collection.insertOne(json("_id: 17, a: null, b: 123"));

        assertThat(collection.find(json("")).sort(json("a: 1, _id: 1")))
            .containsExactly(
                json("_id: 15, a: null, b: 123"),
                json("_id: 16, b: 123"),
                json("_id: 17, a: null, b: 123"),
                json("_id: 13, a: {b: null, c: 0}"),
                json("_id: 10, a: {b: 1}"),
                json("_id:  3, a: {b: 1, c: null}"),
                json("_id:  1, a: {b: 1, c: 0}"),
                json("_id:  2, a: {b: 1, c: 0.0}"),
                json("_id:  4, a: {b: 1.0, c: 0}"),
                json("_id:  5, a: {b: 1.0, c: 0.0}"),
                json("_id:  6, a: {b: 1.0, c: 0}"),
                json("_id:  9, a: {c: 0}"),
                json("_id:  8, a: {c: 0, b: 1.0}"),
                json("_id: 12, a: {c: 2}"),
                json("_id: 14, a: {b: 'abc'}"),
                json("_id: 11, a: {b: {c: 0.0}}"),
                json("_id:  7, a: {b: {c: 1.0}}")
            );
    }

    @Test
    void testFindByListValue() {
        collection.insertOne(json("_id: 1, a: [2, 1]"));
        collection.insertOne(json("_id: 2, a: [2, 1.0]"));
        collection.insertOne(json("_id: 3, a: [1, 2]"));
        collection.insertOne(json("_id: 4, a: [1, 2, 3]"));
        collection.insertOne(json("_id: 5, a: [3, 2]"));
        collection.insertOne(json("_id: 6, a: [2, 3]"));
        collection.insertOne(json("_id: 7, a: [3]"));
        collection.insertOne(json("_id: 8, a: [3, 2]"));

        assertThat(collection.find(json("a: [2, 1]")))
            .containsExactlyInAnyOrder(
                json("_id: 1, a: [2, 1]"),
                json("_id: 2, a: [2, 1.0]")
            );

        assertThat(collection.find(json("a: [1, 2]")))
            .containsExactly(json("_id: 3, a: [1, 2]"));
    }

    @Test
    public void testFindAndOrderByWithListValues() {
        collection.insertOne(json("_id:  1, a: []"));
        collection.insertOne(json("_id:  2, a: null"));
        collection.insertOne(json("_id:  3, a: [2, 1]"));
        collection.insertOne(json("_id:  4, a: [2, 1.0]"));
        collection.insertOne(json("_id:  5, a: [1, 2]"));
        collection.insertOne(json("_id:  6, a: [1, 2, 3]"));
        collection.insertOne(json("_id:  7, a: [3, 2]"));
        collection.insertOne(json("_id:  8, a: [2, 3]"));
        collection.insertOne(json("_id:  9, a: [3]"));
        collection.insertOne(json("_id: 10, a: [3, 2]"));
        collection.insertOne(json("_id: 11, a: [null, 1, 2]"));
        collection.insertOne(json("_id: 12, a: [1, 'abc', 2]"));
        collection.insertOne(json("_id: 13"));
        collection.insertOne(json("_id: 14, a: 'xyz'"));
        collection.insertOne(json("_id: 15, a: {b: 5}"));
        collection.insertOne(json("_id: 16, a: 1"));

        assertThat(collection.find(json("")).sort(json("a: 1, _id: -1")))
            .containsExactly(
                json("_id:  1, a: []"),
                json("_id: 13"),
                json("_id: 11, a: [null, 1, 2]"),
                json("_id:  2, a: null"),
                json("_id: 16, a: 1"),
                json("_id: 12, a: [1, 'abc', 2]"),
                json("_id:  6, a: [1, 2, 3]"),
                json("_id:  5, a: [1, 2]"),
                json("_id:  4, a: [2, 1.0]"),
                json("_id:  3, a: [2, 1]"),
                json("_id: 10, a: [3, 2]"),
                json("_id:  8, a: [2, 3]"),
                json("_id:  7, a: [3, 2]"),
                json("_id:  9, a: [3]"),
                json("_id: 14, a: 'xyz'"),
                json("_id: 15, a: {b: 5}")
            );

        assertThat(collection.find(json("")).sort(json("a: 1, _id: 1")))
            .containsExactly(
                json("_id:  1, a: []"),
                json("_id:  2, a: null"),
                json("_id: 11, a: [null, 1, 2]"),
                json("_id: 13"),
                json("_id:  3, a: [2, 1]"),
                json("_id:  4, a: [2, 1.0]"),
                json("_id:  5, a: [1, 2]"),
                json("_id:  6, a: [1, 2, 3]"),
                json("_id: 12, a: [1, 'abc', 2]"),
                json("_id: 16, a: 1"),
                json("_id:  7, a: [3, 2]"),
                json("_id:  8, a: [2, 3]"),
                json("_id: 10, a: [3, 2]"),
                json("_id:  9, a: [3]"),
                json("_id: 14, a: 'xyz'"),
                json("_id: 15, a: {b: 5}")
            );

        assertThat(collection.find(json("")).sort(json("a: -1, _id: -1")))
            .containsExactly(
                json("_id: 15, a: {b: 5}"),
                json("_id: 14, a: 'xyz'"),
                json("_id: 12, a: [1, 'abc', 2]"),
                json("_id: 10, a: [3, 2]"),
                json("_id:  9, a: [3]"),
                json("_id:  8, a: [2, 3]"),
                json("_id:  7, a: [3, 2]"),
                json("_id:  6, a: [1, 2, 3]"),
                json("_id: 11, a: [null, 1, 2]"),
                json("_id:  5, a: [1, 2]"),
                json("_id:  4, a: [2, 1.0]"),
                json("_id:  3, a: [2, 1]"),
                json("_id: 16, a: 1"),
                json("_id: 13"),
                json("_id:  2, a: null"),
                json("_id:  1, a: []")
            );
    }

    @Test
    void testDistinctEmbeddedDocument() {
        collection.insertOne(json("_id:  1, a: {b: 1, c: 0}"));
        collection.insertOne(json("_id:  2, a: {b: null}"));
        collection.insertOne(json("_id:  3, a: {b: 1, c: null}"));
        collection.insertOne(json("_id:  4, a: {b: 1.0, c: 0}"));
        collection.insertOne(json("_id:  5, a: {b: 1.0, c: 0.0}"));
        collection.insertOne(json("_id:  6, a: {b: 1.0, c: null}"));
        collection.insertOne(json("_id:  7, a: {b: {c: 1.0}}"));
        collection.insertOne(json("_id:  8, a: {c: 0, b: 1.0}"));
        collection.insertOne(json("_id:  9, a: {c: 0, b: null}"));
        collection.insertOne(json("_id: 10, a: {b: 1}"));
        collection.insertOne(json("_id: 11, a: {b: {c: 0.0}}"));
        collection.insertOne(json("_id: 12"));
        collection.insertOne(json("_id: 13, a: {c: 0}"));
        collection.insertOne(json("_id: 14, a: {c: null}"));
        collection.insertOne(json("_id: 15, a: null"));

        assertThat(collection.distinct("a", Document.class))
            .containsExactlyInAnyOrder(
                json("b: 1, c: 0"),
                json("b: null"),
                json("b: 1, c: null"),
                json("b: {c: 1.0}"),
                json("b: 1.0, c: 0"),
                json("b: null, c: 0"),
                json("b: 1"),
                json("b: {c: 0.0}"),
                json("c: 0"),
                json("c: null"),
                null
            );
    }

    @Test
    void testEmptyArrayQuery() {
        collection.insertOne(json("_id: 1"));

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(Filters.and()).first())
            .withMessageContaining("must be a nonempty array");
    }

    @Test
    void testFindAllReferences() {
        collection.insertOne(new Document("_id", 1).append("ref", new DBRef("coll1", 1)));
        collection.insertOne(new Document("_id", 2).append("ref", new DBRef("coll1", 2)));
        collection.insertOne(new Document("_id", 3).append("ref", new DBRef("coll2", 1)));
        collection.insertOne(new Document("_id", 4).append("ref", new DBRef("coll2", 2)));
        collection.insertOne(json("_id: 5, ref: [1, 2, 3, 4]"));
        collection.insertOne(json("_id: 6"));

        assertThat(collection.find(json("ref: {$ref: 'coll1', $id: 1}")).projection(json("_id: 1")))
            .containsExactly(json("_id: 1"));
    }

    @Test
    void testInsertAndQueryNegativeZero() {
        collection.insertOne(json("_id: 1, value: -0.0"));
        collection.insertOne(json("_id: 2, value: 0.0"));
        collection.insertOne(json("_id: 3, value: -0.0"));

        assertThat(collection.find(json("value: -0.0")))
            .containsExactlyInAnyOrder(
                json("_id: 1, value: -0.0"),
                json("_id: 2, value: 0.0"),
                json("_id: 3, value: -0.0")
            );

        assertThat(collection.find(json("value: {$lt: 0.0}"))).isEmpty();

        assertThat(collection.find(json("value: 0")).sort(json("value: 1, _id: 1")))
            .extracting(doc -> doc.getDouble("value"))
            .containsExactly(-0.0, +0.0, -0.0);
    }

    @Test
    void testUniqueIndexWithNegativeZero() {
        collection.createIndex(json("value: 1"), new IndexOptions().unique(true));

        collection.insertOne(json("_id: 1, value: -0.0"));

        assertMongoWriteException(() -> collection.insertOne(json("_id: 2, value: 0.0")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: value_1 dup key: { value: 0.0 }");

        assertMongoWriteException(() -> collection.insertOne(json("_id: 3, value: -0.0")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: value_1 dup key: { value: -0.0 }");

        assertMongoWriteException(() -> collection.insertOne(json("_id: 4, value: 0")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: value_1 dup key: { value: 0 }");
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/45
    @Test
    public void testDecimal128() {
        collection.insertOne(json("_id: {'$numberDecimal': '1'}"));
        collection.insertOne(json("_id: {'$numberDecimal': '2'}"));
        collection.insertOne(json("_id: {'$numberDecimal': '3.0'}"));
        collection.insertOne(json("_id: {'$numberDecimal': '200000000000000000000000000000000.5'}"));

        assertMongoWriteException(() -> collection.insertOne(json("_id: {'$numberDecimal': '1'}")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: _id_ dup key: { _id: 1 }");

        assertThat(collection.find(json("_id: {$eq: {'$numberDecimal': '3'}}")))
            .containsExactly(
                json("_id: {'$numberDecimal': '3.0'}")
            );

        assertThat(collection.find(json("_id: {$gt: {'$numberDecimal': '100000'}}")))
            .containsExactly(
                json("_id: {'$numberDecimal': '200000000000000000000000000000000.5'}")
            );
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/112
    @Test
    public void testDecimal128_Inc() {
        collection.insertOne(json("_id: 1, value: {'$numberDecimal': '1'}"));
        collection.insertOne(json("_id: 2, value: {'$numberDecimal': '2'}"));
        collection.insertOne(json("_id: 3, value: {'$numberDecimal': '3.0'}"));
        collection.insertOne(json("_id: 4, value: {'$numberDecimal': '200000000000000000000000000000000.5'}"));

        collection.updateMany(json(""), json("$inc: {value: 1}"));

        assertThat(collection.find().sort(json("_id: 1")))
            .containsExactly(
                json("_id: 1, value: {'$numberDecimal': '2'}"),
                json("_id: 2, value: {'$numberDecimal': '3'}"),
                json("_id: 3, value: {'$numberDecimal': '4.0'}"),
                json("_id: 4, value: {'$numberDecimal': '200000000000000000000000000000001.5'}")
            );

        collection.updateMany(json(""), json("$inc: {value: {'$numberDecimal': '2.5'}}"));

        assertThat(collection.find().sort(json("_id: 1")))
            .containsExactly(
                json("_id: 1, value: {'$numberDecimal': '4.5'}"),
                json("_id: 2, value: {'$numberDecimal': '5.5'}"),
                json("_id: 3, value: {'$numberDecimal': '6.5'}"),
                json("_id: 4, value: {'$numberDecimal': '200000000000000000000000000000004.0'}")
            );
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/45
    @Test
    public void testArrayNe() {
        collection.insertOne(json("_id: 'a', values: [-1]"));
        collection.insertOne(json("_id: 'b', values: [0]"));
        collection.insertOne(json("_id: 'c', values: 1.0"));
        collection.insertOne(json("_id: 'd', values: {'$numberDecimal': '1.0'}"));
        collection.insertOne(json("_id: 'e', values: {'$numberDecimal': '0.0'}"));
        collection.insertOne(json("_id: 'f', values: [-0.0]"));
        collection.insertOne(json("_id: 'g', values: [0, 1]"));
        collection.insertOne(json("_id: 'h', values: 0.0"));
        collection.insertOne(json("_id: 'i', values: []"));
        collection.insertOne(json("_id: 'j', values: null"));
        collection.insertOne(json("_id: 'k'"));

        assertThat(collection.find(json("values: {$ne: 0}")))
            .containsExactly(
                json("_id: 'a', values: [-1]"),
                json("_id: 'c', values: 1.0"),
                json("_id: 'd', values: {'$numberDecimal': '1.0'}"),
                json("_id: 'i', values: []"),
                json("_id: 'j', values: null"),
                json("_id: 'k'")
            );

        assertThat(collection.find(json("values: {$ne: []}")))
            .containsExactly(
                json("_id: 'a', values: [-1]"),
                json("_id: 'b', values: [0]"),
                json("_id: 'c', values: 1.0"),
                json("_id: 'd', values: {'$numberDecimal': '1.0'}"),
                json("_id: 'e', values: {'$numberDecimal': '0.0'}"),
                json("_id: 'f', values: [-0.0]"),
                json("_id: 'g', values: [0, 1]"),
                json("_id: 'h', values: 0.0"),
                json("_id: 'j', values: null"),
                json("_id: 'k'")
            );
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/48
    @Test
    void testExistsQuery() {
        collection.insertOne(json("_id: 1, a: {b: 1}"));
        collection.insertOne(json("_id: 2, a: null"));
        collection.insertOne(json("_id: 3, a: {b: null}"));
        collection.insertOne(json("_id: 4"));

        assertThat(collection.find(json("'a.b': {$exists: false}")))
            .containsExactlyInAnyOrder(
                json("_id: 2, a: null"),
                json("_id: 4")
            );

        assertThat(collection.find(json("'a.b': {$exists: true}")))
            .containsExactlyInAnyOrder(
                json("_id: 1, a: {b: 1}"),
                json("_id: 3, a: {b: null}")
            );

        assertThat(collection.find(json("a: {b: {$exists: true}}"))).isEmpty();

        assertThat(collection.find(json("a: {$exists: true}")))
            .containsExactlyInAnyOrder(
                json("_id: 1, a: {b: 1}"),
                json("_id: 2, a: null"),
                json("_id: 3, a: {b: null}")
            );

        assertThat(collection.find(json("b: {$exists: true}"))).isEmpty();
    }

    @Test
    void testExistsQueryWithArray() {
        collection.insertOne(json("_id: 1, a: {b: 1}"));
        collection.insertOne(json("_id: 2, a: ['X', 'Y', 'Z']"));
        collection.insertOne(json("_id: 3, a: [[1, 2], [3, 4]]"));
        collection.insertOne(json("_id: 4, a: ['x']"));
        collection.insertOne(json("_id: 5, a: []"));
        collection.insertOne(json("_id: 6, a: null"));
        collection.insertOne(json("_id: 7"));

        assertThat(collection.find(json("'a.1': {$exists: true}")))
            .containsExactlyInAnyOrder(
                json("_id: 2, a: ['X', 'Y', 'Z']"),
                json("_id: 3, a: [[1, 2], [3, 4]]")
            );

        assertThat(collection.find(json("'a.0': {$exists: false}")))
            .containsExactlyInAnyOrder(
                json("_id: 1, a: {b: 1}"),
                json("_id: 5, a: []"),
                json("_id: 6, a: null"),
                json("_id: 7")
            );

        assertThat(collection.find(json("'a.0.1': {$exists: true}")))
            .containsExactlyInAnyOrder(
                json("_id: 3, a: [[1, 2], [3, 4]]")
            );

        assertThat(collection.find(json("'a.0.1': {$exists: false}")))
            .containsExactlyInAnyOrder(
                json("_id: 1, a: {b: 1}"),
                json("_id: 2, a: ['X', 'Y', 'Z']"),
                json("_id: 4, a: ['x']"),
                json("_id: 5, a: []"),
                json("_id: 6, a: null"),
                json("_id: 7")
            );
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/53
    @Test
    void testExistsQueryWithTrailingDot() {
        collection.insertOne(json("_id: 1, a: {b: 1}"));
        collection.insertOne(json("_id: 2, a: ['X', 'Y', 'Z']"));
        collection.insertOne(json("_id: 3, a: [[1, 2], [3, 4]]"));
        collection.insertOne(json("_id: 4, a: ['x']"));
        collection.insertOne(json("_id: 5, a: []"));
        collection.insertOne(json("_id: 6, a: null"));
        collection.insertOne(json("_id: 7"));
        collection.insertOne(json("_id: 8, a: {b: {c: 'd'}}"));

        assertThat(collection.find(json("'a.': {$exists: true}")))
            .containsExactlyInAnyOrder(
                json("_id: 2, a: ['X', 'Y', 'Z']"),
                json("_id: 3, a: [[1, 2], [3, 4]]"),
                json("_id: 4, a: ['x']"),
                json("_id: 5, a: []")
            );

        assertThat(collection.find(json("'a.1.': {$exists: true}")))
            .containsExactlyInAnyOrder(
                json("_id: 3, a: [[1, 2], [3, 4]]")
            );

        assertThat(collection.find(json("'.a': {$exists: true}"))).isEmpty();
        assertThat(collection.find(json("'a.b.': {$exists: true}"))).isEmpty();
        assertThat(collection.find(json("'a..': {$exists: true}"))).isEmpty();
        assertThat(collection.find(json("'a.....111': {$exists: true}"))).isEmpty();

        assertThat(collection.find(json("'a.': {$exists: false}")))
            .containsExactlyInAnyOrder(
                json("_id: 1, a: {b: 1}"),
                json("_id: 6, a: null"),
                json("_id: 7"),
                json("_id: 8, a: {b: {c: 'd'}}")
            );

        assertThat(collection.find(json("'a.1.': {$exists: false}")))
            .containsExactlyInAnyOrder(
                json("_id: 1, a: {b: 1}"),
                json("_id: 2, a: ['X', 'Y', 'Z']"),
                json("_id: 4, a: ['x']"),
                json("_id: 5, a: []"),
                json("_id: 6, a: null"),
                json("_id: 7"),
                json("_id: 8, a: {b: {c: 'd'}}")
            );

        assertThat(collection.find(json("'a..': {$exists: false}")))
            .containsExactlyInAnyOrder(
                json("_id: 1, a: {b: 1}"),
                json("_id: 2, a: ['X', 'Y', 'Z']"),
                json("_id: 3, a: [[1, 2], [3, 4]]"),
                json("_id: 4, a: ['x']"),
                json("_id: 5, a: []"),
                json("_id: 6, a: null"),
                json("_id: 7"),
                json("_id: 8, a: {b: {c: 'd'}}")
            );
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/56
    @Test
    public void testRegExQuery() {
        collection.insertOne(json("_id: 'one', name: 'karl'"));
        collection.insertOne(json("_id: 'two', name: 'Karl'"));
        collection.insertOne(json("_id: 'Three', name: 'KARL'"));
        collection.insertOne(json("_id: null"));
        collection.insertOne(json("_id: 123, name: ['karl', 'john']"));

        assertThat(collection.find(json("_id: {$regex: '^T.+$', $options: 'i'}")))
            .containsExactlyInAnyOrder(
                json("_id: 'two', name: 'Karl'"),
                json("_id: 'Three', name: 'KARL'")
            );

        assertThat(collection.find(json("_id: {$regex: 't.+'}")))
            .containsExactly(
                json("_id: 'two', name: 'Karl'")
            );

        assertThat(collection.find(json("_id: {$regex: '^(one|1.+)$'}")))
            .containsExactly(
                json("_id: 'one', name: 'karl'")
            );

        assertThat(collection.find(json("name: {$regex: 'arl', $options: 'i'}")))
            .containsExactlyInAnyOrder(
                json("_id: 'one', name: 'karl'"),
                json("_id: 'two', name: 'Karl'"),
                json("_id: 'Three', name: 'KARL'"),
                json("_id: 123, name: ['karl', 'john']")
            );
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/67
    @Test
    public void testInsertAndFindJavaScriptContent() {
        collection.insertOne(new Document("_id", 1).append("data", new BsonJavaScript("int i = 0")));

        assertThat(collection.find(json("_id: 1")).first())
            .extracting(document -> document.get("data"))
            .isEqualTo(new Code("int i = 0"));

        assertThat(collection.find(new Document("data", new BsonJavaScript("int i = 0"))).first())
            .extracting(document -> document.get("_id"))
            .isEqualTo(1);
    }

    private void insertAndFindLargeDocument(int numKeyValues, int id) {
        Document document = new Document("_id", id);
        for (int i = 0; i < numKeyValues; i++) {
            document.put("key-" + i, "value-" + i);
        }
        collection.insertOne(document);

        Document persistentDocument = collection.find(new Document("_id", id)).first();
        assertThat(persistentDocument).hasSize(numKeyValues + 1);
    }

    private void insertUpdateInBulk(boolean ordered) {
        List<WriteModel<Document>> ops = new ArrayList<>();

        ops.add(new InsertOneModel<>(json("_id: 1, field: 'x'")));
        ops.add(new InsertOneModel<>(json("_id: 2, field: 'x'")));
        ops.add(new InsertOneModel<>(json("_id: 3, field: 'x'")));
        ops.add(new UpdateManyModel<>(json("field: 'x'"), set("field", "y")));

        BulkWriteResult result = collection.bulkWrite(ops, new BulkWriteOptions().ordered(ordered));

        assertThat(result.getInsertedCount()).isEqualTo(3);
        assertThat(result.getDeletedCount()).isEqualTo(0);
        assertThat(result.getModifiedCount()).isEqualTo(3);
        assertThat(result.getMatchedCount()).isEqualTo(3);

        long totalDocuments = collection.countDocuments();
        assertThat(totalDocuments).isEqualTo(3);

        long documentsWithY = collection.countDocuments(json("field: 'y'"));
        assertThat(documentsWithY).isEqualTo(3);
    }

    private void insertUpdateInBulkNoMatch(boolean ordered) {

        collection.insertOne(json("foo: 'bar'"));

        List<WriteModel<Document>> ops = new ArrayList<>();
        ops.add(new UpdateOneModel<>(ne("foo", "bar"), set("field", "y")));

        BulkWriteResult result = collection.bulkWrite(ops, new BulkWriteOptions().ordered(ordered));

        assertThat(result.getInsertedCount()).isEqualTo(0);
        assertThat(result.getDeletedCount()).isEqualTo(0);
        assertThat(result.getModifiedCount()).isEqualTo(0);
        assertThat(result.getMatchedCount()).isEqualTo(0);
    }

    private void removeInBulk(boolean ordered) {
        DeleteManyModel<Document> deleteOp = new DeleteManyModel<>(json("field: 'y'"));
        BulkWriteResult result = collection.bulkWrite(List.of(deleteOp),
            new BulkWriteOptions().ordered(ordered));

        assertThat(result.getDeletedCount()).isEqualTo(3);
        assertThat(collection.countDocuments()).isZero();
    }

    protected static void assertMongoWriteException(ThrowingCallable callable, int expectedErrorCode, String expectedMessage) {
        assertMongoWriteException(callable, expectedErrorCode, "Location" + expectedErrorCode, expectedMessage);
    }

    protected static void assertMongoWriteException(ThrowingCallable callable, int expectedErrorCode, String expectedCodeName,
                                                    String expectedMessage) {
        assertThatExceptionOfType(MongoWriteException.class)
            .isThrownBy(callable)
            .withMessageContaining(expectedMessage)
            .extracting(e -> e.getError().getCode())
            .isEqualTo(expectedErrorCode);

        Document lastError = db.runCommand(json("getlasterror: 1"));
        assertThat(lastError.getString("codeName")).isEqualTo(expectedCodeName);
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/76
    @Test
    void testInsertWithoutId() {
        DocumentCodec documentCodec = Mockito.spy(new DocumentCodec());
        Mockito.doAnswer(AdditionalAnswers.returnsFirstArg()).when(documentCodec).generateIdIfAbsentFromDocument(Mockito.any());

        MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
            .applyConnectionString(connectionString)
            .codecRegistry(CodecRegistries.fromCodecs(documentCodec))
            .build();

        try (MongoClient mongoClient = MongoClients.create(mongoClientSettings)) {
            MongoDatabase database = mongoClient.getDatabase(db.getName());
            MongoCollection<Document> collection = database.getCollection(getCollectionName());
            collection.insertOne(json("x: 1"));

            assertThat(collection.find(json("x: 1")).first().get("_id"))
                .isInstanceOf(ObjectId.class);
        }

        Mockito.verify(documentCodec).generateIdIfAbsentFromDocument(Mockito.any());
    }

    @Test
    public void testMultikeyIndex_simpleArrayValues() {
        collection.createIndex(json("a: 1"), new IndexOptions().unique(true));

        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2, a: 1"));
        collection.insertOne(json("_id: 3, a: [2, 3]"));
        collection.insertOne(json("_id: 4, a: [4, 5, 4]"));
        collection.insertOne(json("_id: 5, a: [[1, 2], [3, 4]]"));
        collection.insertOne(json("_id: 6, a: [[1, 3], [4, 5]]"));
        collection.insertOne(json("_id: 7, a: [[2, 1], [4, 3]]"));

        assertMongoWriteException(() -> collection.insertOne(json("a: [1]")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: a_1 dup key: { a: 1 }");

        assertMongoWriteException(() -> collection.insertOne(json("a: [6, 1]")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: a_1 dup key: { a: 1 }");

        assertMongoWriteException(() -> collection.insertOne(json("a: [2.0, 4.0]")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: a_1 dup key: { a: 2.0 }");

        assertMongoWriteException(() -> collection.insertOne(json("a: [[1, 4], [3, 4]]")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: a_1 dup key: { a: [ 3, 4 ] }");

        collection.deleteOne(json("_id: 3"));
        collection.insertOne(json("_id: 8, a: [2, 3]"));

        assertMongoWriteException(() -> collection.replaceOne(json("_id: 1"), json("_id: 1, a: [3, 4]")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: a_1 dup key: { a: 3 }");

        collection.replaceOne(json("_id: 4"), json("_id: 4, a: ['x', 'y']"));
        collection.insertOne(json("_id: 9, a: [4, 6]"));

        assertMongoWriteException(() -> collection.updateOne(json("_id: 9"), json("$push: {a: 2}")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: a_1 dup key: { a: 2 }");

        Document result = collection.findOneAndUpdate(json("_id: 8"), json("$pull: {a: 2}"),
            new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
        assertThat(result).isEqualTo(json("_id: 8, a: [3]"));

        result = collection.findOneAndUpdate(json("_id: 9"), json("$push: {a: 2}"),
            new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
        assertThat(result).isEqualTo(json("_id: 9, a: [4, 6, 2]"));

        result = collection.findOneAndUpdate(json("_id: 9"), json("$push: {a: 2}"),
            new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
        assertThat(result).isEqualTo(json("_id: 9, a: [4, 6, 2, 2]"));
    }

    @Test
    public void testCompoundMultikeyIndex_simpleArrayValues() {
        collection.createIndex(json("a: 1, b: 1"), new IndexOptions().unique(true));

        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2, a: 1"));
        collection.insertOne(json("_id: 3, a: [2, 3], b: 1"));
        collection.insertOne(json("_id: 4, a: [4, 5], b: 1"));
        collection.insertOne(json("_id: 5, a: [1, 2, 3, 4, 5], b: 2"));

        assertMongoWriteException(() -> collection.insertOne(json("a: [1]")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: a_1_b_1 dup key: { a: 1, b: null }");

        assertMongoWriteException(() -> collection.insertOne(json("a: [6, 2], b: 1")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: a_1_b_1 dup key: { a: 2, b: 1 }");

        assertMongoWriteException(() -> collection.insertOne(json("a: ['abc'], b: [1, 2, 3]")),
            171, "CannotIndexParallelArrays", "cannot index parallel arrays [b] [a]");

        collection.deleteOne(json("_id: 3"));
        collection.insertOne(json("_id: 6, a: [2, 3], b: 1"));

        assertMongoWriteException(() -> collection.replaceOne(json("_id: 1"), json("_id: 1, a: [3, 4], b: 1")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: a_1_b_1 dup key: { a: 3, b: 1 }");

        collection.replaceOne(json("_id: 4"), json("_id: 4, a: ['x', 'y'], b: 1"));
        collection.insertOne(json("_id: 7, a: [4, 6], b: 1"));
    }

    @Test
    public void testCompoundMultikeyIndex_threeKeys() {
        collection.createIndex(json("b: 1, a: 1, c: 1"), new IndexOptions().unique(true));

        assertMongoWriteException(() -> collection.insertOne(json("b: [1, 2, 3], a: ['abc'], c: ['x', 'y']")),
            171, "CannotIndexParallelArrays", "cannot index parallel arrays [a] [b]");
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/69
    @Test
    public void testCompoundMultikeyIndex_documents() {
        collection.createIndex(json("item: 1, 'stock.size': 1"), new IndexOptions().unique(true));

        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2, item: 'abc'"));
        collection.insertOne(json("_id: 3, item: 'abc', stock: [{size: 'S', color: 'red'}]"));
        collection.insertOne(json("_id: 4, item: 'abc', stock: [{size: 'L', color: 'black'}]"));
        collection.insertOne(json("_id: 5, item: 'abc', stock: [{size: 'M'}, {size: 'XL'}]"));
        collection.insertOne(json("_id: 6, item: 'xyz', stock: [{size: 'S'}, {size: 'M'}]"));
        collection.insertOne(json("_id: 7, item: 'xyz', stock: [1, 2, 3]"));

        assertMongoWriteException(() -> collection.insertOne(json("item: 'abc'")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: item_1_stock.size_1 dup key: { item: \"abc\", stock.size: null }");

        assertMongoWriteException(() -> collection.insertOne(json("item: 'abc', stock: [{color: 'black'}]")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: item_1_stock.size_1 dup key: { item: \"abc\", stock.size: null }");

        assertMongoWriteException(() -> collection.insertOne(json("item: 'abc', stock: [{size: 'S'}]")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: item_1_stock.size_1 dup key: { item: \"abc\", stock.size: \"S\" }");

        assertMongoWriteException(() -> collection.insertOne(json("item: 'abc', stock: [{size: 'XL'}]")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: item_1_stock.size_1 dup key: { item: \"abc\", stock.size: \"XL\" }");
    }

    @Test
    public void testCompoundMultikeyIndex_multiple_document_keys() {
        collection.createIndex(json("item: 1, 'stock.size': 1, 'stock.color': 1"), new IndexOptions().unique(true));

        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2, item: 'abc'"));
        collection.insertOne(json("_id: 4, item: 'abc', stock: [{color: 'red'}]"));
        collection.insertOne(json("_id: 5, item: 'abc', stock: [{size: 'L', color: 'red'}]"));
        collection.insertOne(json("_id: 6, item: 'abc', stock: [{size: 'L'}, {size: 'XL'}]"));
        collection.insertOne(json("_id: 7, item: 'abc', stock: [{size: 'S', color: 'red'}]"));
        collection.insertOne(json("_id: 8, item: 'xyz', stock: [{size: 'S', color: 'red'}]"));
        collection.insertOne(json("_id: 9, item: 'xyz', stock: [1, 2, 3]"));

        assertMongoWriteException(() -> collection.insertOne(json("item: 'abc'")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: item_1_stock.size_1_stock.color_1 dup key: { item: \"abc\", stock.size: null, stock.color: null }");

        assertMongoWriteException(() -> collection.insertOne(json("item: 'abc', stock: [{color: 'red'}]")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: item_1_stock.size_1_stock.color_1 dup key: { item: \"abc\", stock.size: null, stock.color: \"red\" }");

        assertMongoWriteException(() -> collection.insertOne(json("item: 'abc', stock: [{size: 'XL'}]")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: item_1_stock.size_1_stock.color_1 dup key: { item: \"abc\", stock.size: \"XL\", stock.color: null }");
    }

    @Test
    public void testCompoundMultikeyIndex_deepDocuments() {
        collection.createIndex(json("'a.b.c': 1"), new IndexOptions().unique(true));

        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2, a: {b: {c: 1}}"));
        collection.insertOne(json("_id: 3, a: {b: {c: 2}}"));
        collection.insertOne(json("_id: 4, a: [{b: {c: 3}}, {b: {c: 4}}]"));
        collection.insertOne(json("_id: 5, a: [{b: [{c: 5}, {c: 6}]}, {b: [{c: 7}, {c: 8}]}]"));

        assertMongoWriteException(() -> collection.insertOne(json("a: {b: 1}")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: a.b.c_1 dup key: { a.b.c: null }");

        assertMongoWriteException(() -> collection.insertOne(json("a: [{b: 1}, {b: 2}]")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: a.b.c_1 dup key: { a.b.c: null }");

        assertMongoWriteException(() -> collection.insertOne(json("a: {b: {c: 1}}")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: a.b.c_1 dup key: { a.b.c: 1 }");

        assertMongoWriteException(() -> collection.insertOne(json("a: {b: {c: [1, 2]}}")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: a.b.c_1 dup key: { a.b.c: 1 }");

        assertMongoWriteException(() -> collection.insertOne(json("a: {b: {c: 4}}")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: a.b.c_1 dup key: { a.b.c: 4 }");

        assertMongoWriteException(() -> collection.insertOne(json("a: {b: {c: 8}}")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: a.b.c_1 dup key: { a.b.c: 8 }");
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/82
    @Test
    void testUpdateArrayWithPositionalAll() {
        collection.insertOne(json("_id: 1, grades: [95, 102, 90, 150]"));

        collection.findOneAndUpdate(
            json("_id: 1"),
            json("$inc: {'grades.$[]': -10}"));

        assertThat(collection.find(json("_id: 1")).first())
            .isEqualTo(json("_id: 1, grades: [85, 92, 80, 140]"));

        collection.findOneAndUpdate(
            json("_id: 1"),
            json("$set: {'grades.$[]': 'abc'}"));

        assertThat(collection.find(json("_id: 1")).first())
            .isEqualTo(json("_id: 1, grades: ['abc', 'abc', 'abc', 'abc']"));
    }

    @Test
    void testUpdateArrayWithPositionalAll_NullValue() {
        collection.insertOne(json("_id: 1, grades: [1, 2, null, 3]"));

        collection.findOneAndUpdate(
            json("_id: 1"),
            json("$set: {'grades.$[]': 'abc'}"));

        assertThat(collection.find(json("_id: 1")).first())
            .isEqualTo(json("_id: 1, grades: ['abc', 'abc', 'abc', 'abc']"));
    }

    @Test
    void testUpdateArrayWithPositionalAllAndArrayFilter() {
        collection.insertOne(json("_id: 1, grades: [{x: [1, 2, 3]}, {x: [3, 4, 5]}, {x: [1, 2, 3]}]"));

        collection.findOneAndUpdate(
            json("_id: 1"),
            json("$inc: {'grades.$[].x.$[element]': 1}"),
            new FindOneAndUpdateOptions().arrayFilters(List.of(json("element: {$gte: 3}"))));

        assertThat(collection.find(json("_id: 1")).first())
            .isEqualTo(json("_id: 1, grades: [{x: [1, 2, 4]}, {x: [4, 5, 6]}, {x: [1, 2, 4]}]"));
    }

    @Test
    void testUpdateArrayOfDocumentsWithPositionalAll() {
        collection.insertOne(json("_id: 1, grades: [{value: 20}, {value: 30}, {value: 40}]"));

        collection.findOneAndUpdate(
            json("_id: 1"),
            json("$inc: {'grades.$[].value': 10}"));

        assertThat(collection.find(json("_id: 1")).first())
            .isEqualTo(json("_id: 1, grades: [{value: 30}, {value: 40}, {value: 50}]"));

        collection.findOneAndUpdate(
            json("_id: 1"),
            json("$set: {'grades.$[].value': 10}"));

        assertThat(collection.find(json("_id: 1")).first())
            .isEqualTo(json("_id: 1, grades: [{value: 10}, {value: 10}, {value: 10}]"));
    }

    @Test
    void testIllegalUpdateWithPositionalAll() {
        collection.insertOne(json("_id: 1, a: {b: [1, 2, 3]}"));
        collection.insertOne(json("_id: 2, a: {b: 5}"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(json("_id: 1"), json("$set: {'a.$[]': 'abc'}")))
            .withMessageStartingWith("Command execution failed on MongoDB server with error 2 (BadValue): " +
                "'Plan executor error during findAndModify :: caused by :: " +
                "Cannot apply array updates to non-array element a: { b: [ 1, 2, 3 ] }");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(json("_id: 2"), json("$set: {'a.b.$[]': 'abc'}")))
            .withMessageStartingWith("Command execution failed on MongoDB server with error 2 (BadValue): " +
                "'Plan executor error during findAndModify :: caused by :: " +
                "Cannot apply array updates to non-array element b: 5");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(json("_id: 1"), json("$set: {'a.b.$[].c': 'abc'}")))
            .withMessageStartingWith("Command execution failed on MongoDB server with error 28 (PathNotViable): " +
                "'Plan executor error during findAndModify :: caused by :: " +
                "Cannot create field 'c' in element {0: 1}");
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/82
    @Test
    void testUpsertWithPositionalAll() {
        Document result = collection.findOneAndUpdate(json("_id: 1, a: [5, 8]"), json("$set: {'a.$[]': 1}"),
            new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));

        assertThat(result).isEqualTo(json("_id: 1, a: [1, 1]"));
    }

    private static Stream<Arguments> upsertWithNonMatchingFilterTestData() {
        return Stream.of(
            Arguments.of("$and: [{key1: {$eq: 'value1'}}, {key2: {$eq: 'value2'}}]", "dummy: 'dummy', key1: 'value1', key2: 'value2'"),
            Arguments.of("$and: [{key1: {$eq: {sub: 'value1'}}}, {key2: {$eq: 'value2'}}]", "dummy: 'dummy', key1: {sub: 'value1'}, key2: 'value2'"),
            Arguments.of("$and: [{key1: {$gt: 1}}, {key2: 'value2'}]", "dummy: 'dummy', key2: 'value2'"),
            Arguments.of("$and: [{'sub.key1': {$eq: 'value1'}}, {'sub.key2': {$eq: 'value2'}}]", "dummy: 'dummy', sub: {key1: 'value1', key2: 'value2'}"),
            Arguments.of("$and: [{'sub.key1': 'value1'}, {'sub.key2': 'value2'}]", "dummy: 'dummy', sub: {key1: 'value1', key2: 'value2'}"),
            Arguments.of("$or: [{'sub.key1': 'value1'}, {'sub.key1': 'value1b'}]", "dummy: 'dummy'"),
            Arguments.of("$or: [{'sub.key1': 'value1'}]", "dummy: 'dummy', sub: {key1: 'value1'}"),
            Arguments.of("$or: [{key1: 'value1'}, {key2: 'value2'}]", "dummy: 'dummy'")
        );
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/216
    @ParameterizedTest
    @MethodSource("upsertWithNonMatchingFilterTestData")
    void testUpsertAndFilterDoesNotMatch(String filter, String expectedDocument) {
        collection.updateOne(json(filter),
            json("$set: {dummy: 'dummy'}"), new UpdateOptions().upsert(true));

        assertThat(collection.find().projection(json("_id: 0")))
            .containsExactly(json(expectedDocument));
    }

    private static Stream<Arguments> upsertWithIllegalFilterTestData() {
        return Stream.of(
            Arguments.of("$and: [{key1: 'value1'}, {key1: 'value1b'}, {key2: 'value2'}]", "cannot infer query fields to set, path 'key1' is matched twice"),
            Arguments.of("$and: [{sub: 'value1'}, {'sub.key': 'conflict'}]", "cannot infer query fields to set, both paths 'sub.key' and 'sub' are matched"),
            Arguments.of("$and: [{'sub.a': 'value1'}, {'sub.a.b': 'conflict'}]", "cannot infer query fields to set, both paths 'sub.a.b' and 'sub.a' are matched"),
            Arguments.of("$and: [{'sub': 'value1'}, {'sub.a.b': 'conflict'}]", "cannot infer query fields to set, both paths 'sub.a.b' and 'sub' are matched"),
            Arguments.of("$and: [{'sub.key1': 'value1'}, {'sub.key1': 'value1b'}]", "cannot infer query fields to set, path 'sub.key1' is matched twice")
        );
    }

    @ParameterizedTest
    @MethodSource("upsertWithIllegalFilterTestData")
    void testUpsertWithIllegalFilter(String filter, String expectedErrorMessage) {
        assertMongoWriteException(() -> {
            collection.updateOne(json(filter), json("$set: {dummy: 'dummy'}"), new UpdateOptions().upsert(true));
        }, 54, "NotSingleValueField", expectedErrorMessage);
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/164
    @Test
    void testFindOneAndUpdateWithReturnDocumentBeforeWhenDocumentDidNotExist() {
        Document result = collection.findOneAndUpdate(json("_id: 1, a: [5, 8]"), json("$set: {'a.$[]': 1}"),
            new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.BEFORE));

        assertThat(result).isNull();
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/164
    @Test
    void testFindOneAndReplaceWithReturnDocumentBeforeWhenDocumentDidNotExist() {
        Document result = collection.findOneAndReplace(json("_id: 1"), json("_id: 1, a: [5, 8]"),
            new FindOneAndReplaceOptions().upsert(true).returnDocument(ReturnDocument.BEFORE));

        assertThat(result).isNull();
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/164
    @Test
    void testFindOneAndUpdateWithReturnDocumentBeforeWhenDocumentExists() {
        collection.insertOne(json("_id: 1, a: [5, 8]"));

        Document result = collection.findOneAndUpdate(json("_id: 1, a: [5, 8]"), json("$set: {'a.$[]': 1}"),
            new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.BEFORE));

        assertThat(result).isEqualTo(json("_id: 1, a: [5, 8]"));
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/164
    @Test
    void testFindOneAndReplaceWithReturnDocumentBeforeWhenDocumentExists() {
        collection.insertOne(json("_id: 1, a: [5, 8]"));

        Document result = collection.findOneAndReplace(json("_id: 1"), json("_id: 1, a: [3, 3]"),
            new FindOneAndReplaceOptions().upsert(true).returnDocument(ReturnDocument.BEFORE));

        assertThat(result).isEqualTo(json("_id: 1, a: [5, 8]"));
    }

    @Test
    void testUpdateWithMultipleArrayFiltersInOnePath() {
        collection.insertOne(json("_id: 1, grades: [{value: 10, x: [1, 2]}, {value: 20, x: [3, 4]}]"));

        collection.findOneAndUpdate(
            json("_id: 1"),
            json("$set: {'grades.$[element].x.$[]': 'abc'}"),
            new FindOneAndUpdateOptions().arrayFilters(List.of(json("'element.value': {$gt: 10}"))));

        assertThat(collection.find(json("_id: 1")).first())
            .isEqualTo(json("_id: 1, grades: [{value: 10, x: [1, 2]}, {value: 20, x: ['abc', 'abc']}]"));

        collection.findOneAndUpdate(
            json("_id: 1"),
            json("$set: {'grades.0.x.$[element]': 'abc'}"),
            new FindOneAndUpdateOptions().arrayFilters(List.of(json("'element': {$gt: 1}"))));

        assertThat(collection.find(json("_id: 1")).first())
            .isEqualTo(json("_id: 1, grades: [{value: 10, x: [1, 'abc']}, {value: 20, x: ['abc', 'abc']}]"));
    }

    @Test
    void testUpdateArrayWithMultiplePositionalAll() {
        collection.insertOne(json("_id: 1, grades: [[1, 2], [3, 4]]"));
        collection.insertOne(json("_id: 2, grades: [{c: [1, 2]}, {c: [3, 4]}]"));
        collection.insertOne(json("_id: 3, grades: [{c: [1, 2]}, {c: [3, 4]}, {d: [5, 6]}]"));

        collection.findOneAndUpdate(
            json("_id: 1"),
            json("$inc: {'grades.$[].$[]': 1}"));

        assertThat(collection.find(json("_id: 1")).first())
            .isEqualTo(json("_id: 1, grades: [[2, 3], [4, 5]]"));

        collection.findOneAndUpdate(
            json("_id: 2"),
            json("$inc: {'grades.$[].c.$[]': 1}"));

        assertThat(collection.find(json("_id: 2")).first())
            .isEqualTo(json("_id: 2, grades: [{c: [2, 3]}, {c: [4, 5]}]"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(json("_id: 3"), json("$inc: {'grades.$[].c.$[]': 1}")))
            .withMessageStartingWith("Command execution failed on MongoDB server with error 2 (BadValue): " +
                "'Plan executor error during findAndModify :: caused by :: " +
                "The path 'grades.2.c' must exist in the document in order to apply array updates.");
    }

    @Test
    void testUpdateArrayWithMultiplePositionalAll_Simple() {
        collection.insertOne(json("_id: 1, grades: [[1, 2], [3, 4]]"));

        collection.findOneAndUpdate(
            json("_id: 1"),
            json("$set: {'grades.$[].$[]': 1}"));

        assertThat(collection.find(json("_id: 1")).first())
            .isEqualTo(json("_id: 1, grades: [[1, 1], [1, 1]]"));
    }

    @Test
    void testUpdateArrayWithIllegalMultiplePositionalAll() {
        collection.insertOne(json("_id: 1, grades: [[[1, 2], [3, 4]], [[4, 5], [2, 3]]]"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(
                json("_id: 1"),
                json("$inc: {'grades.$[].$[]': 1}")))
            .withMessageStartingWith("Command execution failed on MongoDB server with error 14 (TypeMismatch): '" +
                "Plan executor error during findAndModify :: caused by :: " +
                "Cannot apply $inc to a value of non-numeric type. {_id: 1} has the field '0' of non-numeric type array");
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/98
    @Test
    public void testGetKeyValues_multiKey_document_nested_objects() {
        collection.createIndex(json("'stock.size': 1, 'stock.quantity': 1"), new IndexOptions().unique(true));

        collection.insertOne(json("stock: [{size: 'S', quantity: 10}]"));
        collection.insertOne(json("stock: [{size: 'M', quantity: 10}, {size: 'L', quantity: 10}]"));
        collection.insertOne(json("stock: [{size: 'S', quantity: 20}]"));
        collection.insertOne(json("stock: [{quantity: 20}]"));
        collection.insertOne(json("stock: [{size: 'M'}]"));
        collection.insertOne(json("stock: {size: ['XL', 'XXL']}"));

        assertThatExceptionOfType(MongoWriteException.class)
            .isThrownBy(() -> collection.insertOne(json("stock: {size: ['S', 'M'], quantity: [30, 40]}")))
            .withMessageContaining("cannot index parallel arrays [quantity] [size]");

        assertMongoWriteException(() -> collection.insertOne(json("stock: {size: 'S', quantity: 10}")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: stock.size_1_stock.quantity_1 dup key: { stock.size: \"S\", stock.quantity: 10 }");

        assertMongoWriteException(() -> collection.insertOne(json("stock: [{size: 'XL', quantity: 7}, {size: 'M', quantity: 10}]")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: stock.size_1_stock.quantity_1 dup key: { stock.size: \"M\", stock.quantity: 10 }");

        assertMongoWriteException(() -> collection.insertOne(json("stock: [{size: 'M'}]")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: stock.size_1_stock.quantity_1 dup key: { stock.size: \"M\", stock.quantity: null }");

        assertMongoWriteException(() -> collection.insertOne(json("stock: {size: 'XL'}")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: stock.size_1_stock.quantity_1 dup key: { stock.size: \"XL\", stock.quantity: null }");

        assertMongoWriteException(() -> collection.insertOne(json("stock: [{quantity: 20}]")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: stock.size_1_stock.quantity_1 dup key: { stock.size: null, stock.quantity: 20 }");
    }

    @Test
    void testComparisons() {
        collection.insertOne(json("_id: 1, a: 'x'"));
        collection.insertOne(json("_id: 2, a: 10"));
        collection.insertOne(json("_id: 3, a: 1"));
        collection.insertOne(json("_id: 4, a: null"));
        collection.insertOne(json("_id: 5"));

        assertThat(collection.find(json("a: {$gt: 1}")))
            .containsExactlyInAnyOrder(
                json("_id: 2, a: 10")
            );

        assertThat(collection.find(json("a: {$gte: 1}")))
            .containsExactlyInAnyOrder(
                json("_id: 2, a: 10"),
                json("_id: 3, a: 1")
            );

        assertThat(collection.find(json("a: {$lt: 10}")))
            .containsExactlyInAnyOrder(
                json("_id: 3, a: 1")
            );

        assertThat(collection.find(json("a: {$lte: 10}")))
            .containsExactlyInAnyOrder(
                json("_id: 2, a: 10"),
                json("_id: 3, a: 1")
            );
    }

    @Test
    void testMinKeyComparison() {
        collection.insertOne(json("_id: 1, value: null"));
        collection.insertOne(json("_id: 2, value: 123"));
        collection.insertOne(json("_id: 3").append("value", new MinKey()));
        collection.insertOne(json("_id: 4"));
        collection.insertOne(json("_id: 5").append("value", new MaxKey()));

        assertThat(collection.find(new Document("value", new Document("$gt", new MinKey()))))
            .containsExactlyInAnyOrder(
                json("_id: 1, value: null"),
                json("_id: 2, value: 123"),
                json("_id: 4"),
                json("_id: 5").append("value", new MaxKey())
            );

        assertThat(collection.find(new Document("value", new Document("$gte", new MinKey()))))
            .containsExactlyInAnyOrder(
                json("_id: 1, value: null"),
                json("_id: 2, value: 123"),
                json("_id: 3").append("value", new MinKey()),
                json("_id: 4"),
                json("_id: 5").append("value", new MaxKey())
            );

        assertThat(collection.find(new Document("value", new Document("$lte", new MinKey()))))
            .containsExactly(json("_id: 3").append("value", new MinKey()));

        assertThat(collection.find(new Document("value", new Document("$lt", new MinKey()))))
            .isEmpty();
    }

    @Test
    void testMaxKeyComparison() {
        collection.insertOne(json("_id: 1, value: null"));
        collection.insertOne(json("_id: 2, value: 123"));
        collection.insertOne(json("_id: 3").append("value", new MaxKey()));
        collection.insertOne(json("_id: 4"));
        collection.insertOne(json("_id: 5").append("value", new MinKey()));

        assertThat(collection.find(new Document("value", new Document("$lt", new MaxKey()))))
            .containsExactlyInAnyOrder(
                json("_id: 1, value: null"),
                json("_id: 2, value: 123"),
                json("_id: 4"),
                json("_id: 5").append("value", new MinKey())
            );

        assertThat(collection.find(new Document("value", new Document("$lte", new MaxKey()))))
            .containsExactlyInAnyOrder(
                json("_id: 1, value: null"),
                json("_id: 2, value: 123"),
                json("_id: 3").append("value", new MaxKey()),
                json("_id: 4"),
                json("_id: 5").append("value", new MinKey())
            );

        assertThat(collection.find(new Document("value", new Document("$gte", new MaxKey()))))
            .containsExactly(json("_id: 3").append("value", new MaxKey()));

        assertThat(collection.find(new Document("value", new Document("$gt", new MaxKey()))))
            .isEmpty();
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/140
    @Test
    public void testMinMaxKeyRangeQuery() {
        collection.insertOne(json("_id: {id1: 10, id2: 20}"));
        collection.insertOne(json("_id: {id1: 20, id2: 50}"));
        collection.insertOne(json("_id: {id1: 10, id2: 100}"));
        collection.insertOne(json("_id: {id1: 10}"));
        collection.insertOne(json("_id: {id1: 10, id2: null}"));
        collection.insertOne(json("_id: {id2: 20}"));

        Document rangeQuery = json("{_id: {'$gt': {id1: 10, id2: {'$minKey': 1}}, '$lt': {id1: 10, id2: {'$maxKey': 1}}}}");

        assertThat(collection.find(rangeQuery).sort(json("_id: 1")))
            .containsExactly(
                json("_id: {id1: 10, id2: null}"),
                json("_id: {id1: 10, id2: 20}"),
                json("_id: {id1: 10, id2: 100}")
            );

        assertThat(collection.find(rangeQuery).sort(json("_id: -1")))
            .containsExactly(
                json("_id: {id1: 10, id2: 100}"),
                json("_id: {id1: 10, id2: 20}"),
                json("_id: {id1: 10, id2: null}")
            );
    }

    @Test
    public void testOldAndNewUuidTypes() {
        Document document1 = new Document("_id", UUID.fromString("5542cbb9-7833-96a2-b456-f13b6ae1bc80"));

        MongoClientSettings legacyUuidSettings = MongoClientSettings.builder()
            .applyConnectionString(connectionString)
            .uuidRepresentation(UuidRepresentation.JAVA_LEGACY)
            .build();

        try (MongoClient clientWithLegacyUuid = MongoClients.create(legacyUuidSettings)) {
            MongoCollection<Document> collectionWithLegacyUuid = clientWithLegacyUuid.getDatabase(collection.getNamespace().getDatabaseName()).getCollection(collection.getNamespace().getCollectionName());
            collectionWithLegacyUuid.insertOne(document1);

            assertMongoWriteException(() -> collectionWithLegacyUuid.insertOne(document1),
                11000, null, "E11000 duplicate key error collection: testdb.testcoll index: _id_ dup key: { _id: BinData(3, A2963378B9CB425580BCE16A3BF156B4) }");

            collection.insertOne(document1);

            assertMongoWriteException(() -> collection.insertOne(document1),
                11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: _id_ dup key: { _id: UUID(\"5542cbb9-7833-96a2-b456-f13b6ae1bc80\") }");

            Document document2 = new Document("_id", UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"));
            collection.insertOne(document2);

            collectionWithLegacyUuid.deleteOne(document1);

            assertThat(collection.find().sort(json("_id: 1")))
                .containsExactly(
                    document1,
                    document2
                );
        }
    }

    @ParameterizedTest
    @EnumSource(UuidRepresentation.class)
    void testUuidRepresentations(UuidRepresentation uuidRepresentation) {
        assumeTrue(uuidRepresentation != UuidRepresentation.UNSPECIFIED);
        MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
            .applyConnectionString(connectionString)
            .uuidRepresentation(uuidRepresentation)
            .build();
        try (MongoClient mongoClient = MongoClients.create(mongoClientSettings)) {
            MongoDatabase database = mongoClient.getDatabase("testdb");
            database.drop();
            MongoCollection<Document> collection = database.getCollection("testcollection");
            collection.insertOne(json("_id: 1").append("key", UUID.fromString("5542cbb9-7833-96a2-b456-f13b6ae1bc80")));

            assertThat(collection.find().first())
                .hasToString("Document{{_id=1, key=5542cbb9-7833-96a2-b456-f13b6ae1bc80}}");
        }
    }

    @Test
    void testConnectionStatus() {
        Document result = runCommand("connectionStatus");
        assertThat(result).isEqualTo(json("ok: 1.0, authInfo: {authenticatedUsers: [], authenticatedUserRoles: []}"));
    }

    @Test
    void testHostInfo() {
        Document result = runCommand("hostInfo");
        assertThat(result.get("ok")).isEqualTo(1.0);
        assertThat(result).containsKeys("os", "system", "extra");
        assertThat(result.get("system", Document.class)).containsKeys("currentTime", "hostname", "numCores", "cpuArch");
    }

    @Test
    void testGetCmdLineOpts() {
        Document result = runCommand("getCmdLineOpts");
        assertThat(result.get("ok")).isEqualTo(1.0);
        assertThat(result).containsOnlyKeys("ok", "argv", "parsed");
    }

    @Test
    void testGetFreeMonitoringStatus() {
        Document result = runCommand("getFreeMonitoringStatus");
        assertThat(result).isEqualTo(json("ok: 1.0, state: 'disabled', debug: {state: 'undecided'}," +
            " message: 'Free monitoring is deprecated, refer to \\'debug\\' field for actual status'"));
    }

    @Test
    void testUpdateWithDollarFieldNames() {
        collection.insertOne(json("_id: 1"));

        collection.updateOne(json("_id: 1"), json("$set: {x: {$expr: {$add: ['$_id', 10]}}}"));

        assertThat(collection.find())
            .containsExactly(json("_id: 1, x: {$expr: {$add: ['$_id', 10]}}"));
    }

    @Test
    void testEndSessions() {
        Document result = getAdminDb().runCommand(new Document("endSessions",
            List.of(new Document("id", UUID.randomUUID()))));
        assertThat(result.get("ok")).isEqualTo(1.0);
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/192
    @Test
    void testLongIndex() {
        long id1 = 223372036854775806L;
        long id2 = 223372036854775800L;
        // GIVEN there are no items in the collection having the given ids
        assertThat(collection.find(Filters.eq(id1)).first()).isNull();
        assertThat(collection.find(Filters.eq(id2)).first()).isNull();

        // WHEN we insert an item with id #1
        collection.insertOne(new Document("_id", id1).append("name", "item 1"));

        // THEN the collections has the item
        assertThat(collection.find(Filters.eq(id1)).first()).isNotNull();
        // AND the collection DOES NOT have an item with id #2
        assertThat(collection.find(Filters.eq(id2)).first()).isNull();
    }

    @Test
    void testQueryWithLargeLongValue() {
        collection.insertOne(new Document("_id", 223372036854775806L).append("name", "item 1"));
        collection.insertOne(new Document("_id", 223372036854775807L).append("name", "item 2"));
        collection.insertOne(new Document("_id", 223372036854775808L).append("name", "item 3"));
        collection.insertOne(new Document("_id", 10.5).append("name", "item 4"));

        assertThat(collection.find(Filters.lt("_id", 223372036854775807L)))
            .containsExactlyInAnyOrder(
                json("_id: 223372036854775806, name: 'item 1'"),
                json("_id: 10.5, name: 'item 4'")
            );

        assertThat(collection.find(Filters.lte("_id", 223372036854775807L)))
            .containsExactlyInAnyOrder(
                json("_id: 223372036854775806, name: 'item 1'"),
                json("_id: 223372036854775807, name: 'item 2'"),
                json("_id: 10.5, name: 'item 4'")
            );

        assertThat(collection.find(Filters.gt("_id", 223372036854775807L)))
            .containsExactly(json("_id: 223372036854775808, name: 'item 3'"));

        assertThat(collection.find(Filters.gte("_id", 223372036854775807L)))
            .containsExactlyInAnyOrder(
                json("_id: 223372036854775807, name: 'item 2'"),
                json("_id: 223372036854775808, name: 'item 3'")
            );
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/195
    @Test
    void testCreateIndicesAndInsertDocumentsConcurrently(TestInfo testInfo) throws Exception {
        int numberOfThreads = 4;
        int numberOfDocumentsPerThread = 10;
        ThreadFactory threadFactory = new CustomizableThreadFactory(testInfo.getDisplayName());
        ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads, threadFactory);
        try {
            for (int repetition = 0; repetition < 50; repetition++) {
                List<Future<?>> futures = new ArrayList<>();
                for (int i = 0; i < numberOfThreads; i++) {
                    int baseIndex = (i + 1) * 100;
                    futures.add(executorService.submit(() -> {
                        collection.createIndex(json("data: 1"));
                        for (int j = 0; j < numberOfDocumentsPerThread; j++) {
                            Document document = new Document("_id", baseIndex + j).append("data", "abc");
                            collection.insertOne(document);
                        }
                        return null;
                    }));
                }

                for (Future<?> future : futures) {
                    future.get(DEFAULT_TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                }

                assertThat(collection.listIndexes())
                    .containsExactlyInAnyOrder(
                        json("key: {_id: 1}, name: '_id_', v: 2"),
                        json("key: {data: 1}, name: 'data_1', v: 2")
                    );

                assertThat(collection.countDocuments()).isEqualTo(numberOfThreads * numberOfDocumentsPerThread);

                db.drop();
            }
        } finally {
            executorService.shutdown();
            boolean success = executorService.awaitTermination(DEFAULT_TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            assertThat(success).isTrue();
        }
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/197
    @Test
    void testFindDocumentByNestedByArray() {
        Document document1 = new Document("_id", new Binary(new byte[] { 0x20, 0x21, 0x22 }));
        Document document2 = new Document("_id", new Binary(new byte[] { 0x21, 0x22, 0x23 }));

        collection.insertOne(document1);
        collection.insertOne(document2);

        assertThat(collection.find(document1))
            .containsExactly(document1);

        assertThat(collection.find(document2))
            .containsExactly(document2);
    }

    @Test
    void testInsertDuplicate_byteArray() {
        Document document = new Document("_id", new Binary(new byte[] { 0x20, 0x21, 0x22 }));

        collection.insertOne(document);

        assertMongoWriteException(() -> collection.insertOne(document), 11000, "DuplicateKey",
            "E11000 duplicate key error collection: testdb.testcoll index: _id_ dup key: { _id: BinData(0, 202122) }");
    }

}
