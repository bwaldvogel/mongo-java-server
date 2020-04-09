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
import static org.assertj.core.api.Assertions.fail;

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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.bson.BsonInt32;
import org.bson.BsonJavaScript;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.UuidCodec;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DBRef;
import com.mongodb.DuplicateKeyException;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoCursorNotFoundException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoQueryException;
import com.mongodb.MongoServerException;
import com.mongodb.MongoWriteException;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.BulkWriteUpsert;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.DeleteManyModel;
import com.mongodb.client.model.EstimatedDocumentCountOptions;
import com.mongodb.client.model.Filters;
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
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.Success;

public abstract class AbstractBackendTest extends AbstractTest {

    private static final Logger log = LoggerFactory.getLogger(AbstractBackendTest.class);

    protected static final String OTHER_TEST_DATABASE_NAME = "bar";

    protected Document runCommand(String commandName) {
        return runCommand(new Document(commandName, 1));
    }

    private Document runCommand(Document command) {
        return getAdminDb().runCommand(command);
    }

    protected MongoCollection<Document> getCollection(String collectionName) {
        return db.getCollection(collectionName);
    }

    protected MongoDatabase getAdminDb() {
        return syncClient.getDatabase("admin");
    }

    private String getCollectionName() {
        return collection.getNamespace().getCollectionName();
    }

    protected static MongoClient getClientWithStandardUuid() {
        CodecRegistry standardUuidCodec = CodecRegistries.fromCodecs(new UuidCodec(UuidRepresentation.STANDARD));
        MongoClientOptions options = MongoClientOptions.builder()
            .codecRegistry(CodecRegistries.fromRegistries(standardUuidCodec, MongoClient.getDefaultCodecRegistry()))
            .build();
        return new MongoClient(new ServerAddress(serverAddress), options);
    }

    @Test
    public void testSimpleInsert() throws Exception {
        collection.insertOne(json("_id: 1"));
    }

    @Test
    public void testSimpleCursor() {
        int expectedCount = 20;
        int batchSize = 10;
        for (int i = 0; i < expectedCount; i++) {
            collection.insertOne(new Document("name", "testUser1"));
        }
        MongoCursor<Document> cursor = collection.find().batchSize(batchSize).cursor();
        int count = 0;
        while (cursor.hasNext()) {
            cursor.next();
            count++;
        }
        assertThat(count).isEqualTo(expectedCount);
        Assertions.assertThrows(NoSuchElementException.class, cursor::next);
    }

    @Test
    public void testCloseCursor() {
        int expectedCount = 20;
        int batchSize = 5;
        for (int i = 0; i < expectedCount; i++) {
            collection.insertOne(new Document("name", "testUser1"));
        }
        MongoCursor<Document> cursor = collection.find().batchSize(batchSize).cursor();
        int count = 0;
        while (cursor.hasNext() && count < 10) {
            cursor.next();
            count++;
        }
        cursor.close();
        assertThat(count).isEqualTo(10);
        Assertions.assertThrows(IllegalStateException.class, cursor::next);
    }

    @Test
    public void testSimpleInsertDelete() throws Exception {
        collection.insertOne(json("_id: 1"));
        collection.deleteOne(json("_id: 1"));
    }

    @Test
    public void testCreateCollection() throws Exception {
        String newCollectionName = "some-collection";
        assertThat(db.listCollectionNames()).doesNotContain(newCollectionName);
        db.createCollection(newCollectionName, new CreateCollectionOptions());
        assertThat(db.listCollectionNames()).contains(newCollectionName);
    }

    @Test
    public void testCreateCollectionAlreadyExists() throws Exception {
        db.createCollection("some-collection", new CreateCollectionOptions());

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> db.createCollection("some-collection", new CreateCollectionOptions()))
            .withMessageContaining("Command failed with error 48 (NamespaceExists): 'a collection 'testdb.some-collection' already exists'");
    }

    @Test
    public void testUnsupportedModifier() throws Exception {
        collection.insertOne(json(""));
        assertMongoWriteException(() -> collection.updateOne(json(""), json("$foo: {}")),
            9, "FailedToParse", "Unknown modifier: $foo. Expected a valid update modifier or pipeline-style update specified as an array");
    }

    @Test
    public void testUpsertWithInc() {
        Document query = json("_id: {f: 'ca', '1': {l: 2}, t: {t: 11}}");
        Document update = json("'$inc': {'n.!' : 1 , 'n.a.b:false' : 1}");

        collection.updateOne(query, update, new UpdateOptions().upsert(true));

        query.putAll(json("n: {'!': 1, a: {'b:false': 1}}"));
        assertThat(collection.find().first()).isEqualTo(query);
    }

    @Test
    public void testBasicUpdate() {
        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2, b: 5"));
        collection.insertOne(json("_id: 3"));
        collection.insertOne(json("_id: 4"));

        collection.replaceOne(json("_id: 2"), json("_id: 2, a: 5"));

        assertThat(collection.find(json("_id: 2")).first()).isEqualTo(json("_id: 2, a: 5"));
    }

    @Test
    public void testCollectionStats_newCollection() throws Exception {
        Document stats = getCollStats();
        assertThat(stats.getDouble("ok")).isEqualTo(1.0);
        assertThat(stats.getInteger("count")).isEqualTo(0);
        assertThat(stats.getInteger("size")).isEqualTo(0);
        assertThat(stats).doesNotContainKey("avgObjSize");

        assertThat(db.listCollectionNames()).doesNotContain(getCollectionName());
    }

    @Test
    public void testCollectionStats() throws Exception {
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
    public void testGetLogStartupWarnings() throws Exception {
        Document startupWarnings = getAdminDb().runCommand(json("getLog: 'startupWarnings'"));
        assertThat(startupWarnings.getDouble("ok")).isEqualTo(1.0);
        assertThat(startupWarnings.get("totalLinesWritten")).isInstanceOf(Number.class);
        assertThat(startupWarnings.get("log")).isEqualTo(Collections.emptyList());
    }

    @Test
    public void testGetLogWhichDoesNotExist() throws Exception {
        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> getAdminDb().runCommand(json("getLog: 'illegal'")))
            .withMessageContaining("Command failed with error -1: 'no RamLog named: illegal'");
    }

    @Test
    public void testCompoundDateIdUpserts() {
        Document query = json("_id: {$lt: {n: 'a', t: 10}, $gte: {n: 'a', t: 1}}");

        List<Document> toUpsert = Arrays.asList(
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
    public void testCompoundSort() {
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
    public void testCountCommand() {
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
    public void testNonPrimaryCountCommand() {
        assertThat(collection.withReadPreference(ReadPreference.nearest()).countDocuments()).isZero();
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testCountCommandWithQuery() {
        collection.insertOne(json("n:1"));
        collection.insertOne(json("n:2"));
        collection.insertOne(json("n:2"));
        assertThat(collection.count(json("n:2"))).isEqualTo(2);
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testCountCommandWithSkipAndLimit() {
        collection.insertOne(json("x: 1"));
        collection.insertOne(json("x: 1"));
        collection.insertOne(json("x: 2"));
        collection.insertOne(json("x: 1"));
        collection.insertOne(json("x: 2"));
        collection.insertOne(json("x: 1"));

        assertThat(collection.count(json("x: 1"), new CountOptions().skip(4).limit(2))).isEqualTo(0);
        assertThat(collection.count(json("x: 1"), new CountOptions().limit(3))).isEqualTo(3);
        assertThat(collection.count(json("x: 1"), new CountOptions().limit(10))).isEqualTo(4);
        assertThat(collection.count(json("x: 1"), new CountOptions().skip(1))).isEqualTo(3);
    }

    @Test
    public void testCountDocuments() throws Exception {
        assertThat(collection.countDocuments()).isZero();
    }

    @Test
    public void testCountDocumentsWithQuery() {
        collection.insertOne(json("n:1"));
        collection.insertOne(json("n:2"));
        collection.insertOne(json("n:2"));
        assertThat(collection.countDocuments(json("n:2"))).isEqualTo(2);
    }

    @Test
    public void testEstimatedDocumentCount() throws Exception {
        assertThat(collection.estimatedDocumentCount()).isEqualTo(0);
        collection.insertOne(json("n:1"));
        collection.insertOne(json("n:2"));
        collection.insertOne(json("n:2"));
        assertThat(collection.estimatedDocumentCount()).isEqualTo(3);
        assertThat(collection.estimatedDocumentCount(new EstimatedDocumentCountOptions().maxTime(1, TimeUnit.SECONDS))).isEqualTo(3);
    }

    @Test
    public void testCreateIndexes() {
        collection.createIndex(new Document("n", 1));
        collection.createIndex(new Document("b", 1));

        assertThat(collection.listIndexes())
            .containsExactlyInAnyOrder(
                json("key: {_id: 1}").append("ns", collection.getNamespace().getFullName()).append("name", "_id_").append("v", 2),
                json("key: {n: 1}").append("ns", collection.getNamespace().getFullName()).append("name", "n_1").append("v", 2),
                json("key: {b: 1}").append("ns", collection.getNamespace().getFullName()).append("name", "b_1").append("v", 2)
            );
    }

    @Test
    public void testDropAndRecreateIndex() throws Exception {
        collection.createIndex(new Document("n", 1));
        collection.createIndex(new Document("b", 1));
        collection.createIndex(new Document("c", 1), new IndexOptions().unique(true));

        collection.dropIndex(new Document("n", 1));

        collection.insertOne(json("_id: 1, c: 10"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.dropIndex(new Document("n", 1)))
            .withMessageContaining("Command failed with error 27 (IndexNotFound): 'can't find index with key: { n: 1 }'");

        assertMongoWriteException(() -> collection.insertOne(json("_id: 2, c: 10")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: c_1 dup key: { c: 10 }");

        collection.dropIndex(new Document("c", 1));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.dropIndex(new Document("c", 1)))
            .withMessageContaining("Command failed with error 27 (IndexNotFound): 'can't find index with key: { c: 1 }'");

        assertThat(collection.listIndexes())
            .containsExactlyInAnyOrder(
                json("key: {_id: 1}").append("ns", collection.getNamespace().getFullName()).append("name", "_id_").append("v", 2),
                json("key: {b: 1}").append("ns", collection.getNamespace().getFullName()).append("name", "b_1").append("v", 2)
            );

        collection.insertOne(json("_id: 2, c: 10"));

        assertThatExceptionOfType(DuplicateKeyException.class)
            .isThrownBy(() -> collection.createIndex(new Document("c", 1), new IndexOptions().unique(true)))
            .withMessageContaining("Write failed with error code 11000 and error message " +
                "'E11000 duplicate key error collection: testdb.testcoll index: c_1 dup key:");

        collection.deleteOne(json("_id: 1"));
        collection.createIndex(new Document("c", 1), new IndexOptions().unique(true));

        assertThat(collection.listIndexes())
            .containsExactlyInAnyOrder(
                json("key: {_id: 1}").append("ns", collection.getNamespace().getFullName()).append("name", "_id_").append("v", 2),
                json("key: {b: 1}").append("ns", collection.getNamespace().getFullName()).append("name", "b_1").append("v", 2),
                json("key: {c: 1}").append("ns", collection.getNamespace().getFullName()).append("name", "c_1").append("unique", true).append("v", 2)
            );
    }

    @Test
    public void testCurrentOperations() throws Exception {
        Document currentOperations = getAdminDb().getCollection("$cmd.sys.inprog").find().first();
        assertThat(currentOperations).isNotNull();
        assertThat(currentOperations.get("inprog")).isInstanceOf(List.class);
    }

    @Test
    public void testListCollectionsEmpty() throws Exception {
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
    public void testListCollections() throws Exception {
        List<String> collections = Arrays.asList("coll1", "coll2", "coll3");
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
            assertThat(collection.get("idIndex")).isEqualTo(json("key: {_id: 1}, name: '_id_', ns: 'testdb." + name + "', v: 2"));
            assertThat(collection.get("info")).isInstanceOf(Document.class);
            collectionNames.add(name);
        }

        assertThat(collectionNames).containsExactlyInAnyOrderElementsOf(collections);
    }

    @Test
    public void testGetCollectionNames() throws Exception {
        getCollection("foo").insertOne(json(""));
        getCollection("bar").insertOne(json(""));

        assertThat(db.listCollectionNames())
            .containsExactlyInAnyOrder("foo", "bar");
    }

    @Test
    public void testSystemNamespaces() throws Exception {
        getCollection("foo").insertOne(json(""));
        getCollection("bar").insertOne(json(""));

        MongoCollection<Document> namespaces = db.getCollection("system.namespaces");
        assertThat(namespaces.find()).containsExactlyInAnyOrder(
            json("name: 'testdb.foo'"),
            json("name: 'testdb.bar'")
        );
    }

    @Test
    public void testDatabaseStats() throws Exception {
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
    public void testDeleteDecrementsCount() {
        collection.insertOne(json("key: 'value'"));
        assertThat(collection.countDocuments()).isEqualTo(1);
        collection.deleteOne(json(""));
        assertThat(collection.countDocuments()).isZero();
    }

    @Test
    public void testDeleteInSystemNamespace() throws Exception {
        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> getCollection("system.foobar").deleteOne(json("")))
            .withMessageContaining("Command failed with error 73 (InvalidNamespace): 'cannot write to 'testdb.system.foobar'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> getCollection("system.namespaces").deleteOne(json("")))
            .withMessageContaining("Command failed with error 73 (InvalidNamespace): 'cannot write to 'testdb.system.namespaces'");
    }

    @Test
    public void testUpdateInSystemNamespace() throws Exception {
        for (String collectionName : Arrays.asList("system.foobar", "system.namespaces")) {
            MongoCollection<Document> collection = getCollection(collectionName);

            assertMongoWriteException(() -> collection.updateMany(eq("some", "value"), set("field", "value")),
                10156, "cannot update system collection");
        }
    }

    @Test
    public void testDistinctQuery() {
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
    public void testDistinctUuids_legacy() throws Exception {
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

    @Test
    public void testDistinctUuids() throws Exception {
        try (MongoClient standardUuidClient = getClientWithStandardUuid()) {
            MongoCollection<Document> standardUuidCollection = standardUuidClient.getDatabase(collection.getNamespace().getDatabaseName()).getCollection(collection.getNamespace().getCollectionName());
            standardUuidCollection.insertOne(json("_id: 1, n: null"));
            standardUuidCollection.insertOne(json("_id: 2").append("n", new UUID(0, 1)));
            standardUuidCollection.insertOne(json("_id: 3").append("n", new UUID(1, 0)));
            standardUuidCollection.insertOne(json("_id: 4").append("n", new UUID(0, 2)));
            standardUuidCollection.insertOne(json("_id: 5").append("n", new UUID(1, 1)));
            standardUuidCollection.insertOne(json("_id: 6").append("n", new UUID(1, 0)));

            assertThat(standardUuidCollection.distinct("n", UUID.class))
                .containsExactly(
                    null,
                    new UUID(0, 1),
                    new UUID(0, 2),
                    new UUID(1, 0),
                    new UUID(1, 1)
                );
        }
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/70
    @Test
    public void testDistinctArrayField() throws Exception {
        collection.insertOne(json("_id: 1, n: null"));
        collection.insertOne(json("_id: 2").append("n", Arrays.asList(1, 2, 3)));
        collection.insertOne(json("_id: 3").append("n", Arrays.asList(3, 4, 5)));
        collection.insertOne(json("_id: 4").append("n", 6));

        assertThat(collection.distinct("n", Integer.class))
            .containsExactly(null, 1, 2, 3, 4, 5, 6);
    }

    @Test
    public void testDistinct_documentArray() throws Exception {
        collection.insertOne(json("_id: 1, n: null"));
        collection.insertOne(json("_id: 2, n: [{item: 1}, {item: 2}]"));
        collection.insertOne(json("_id: 3, n: {item: 3}"));
        collection.insertOne(json("_id: 4, n: {item: [4, 5]}"));
        collection.insertOne(json("_id: 5, n: {}"));

        assertThat(collection.distinct("n.item", Integer.class))
            .containsExactly(1, 2, 3, 4, 5);
    }

    @Test
    public void testInsertQueryAndSortBinaryTypes() throws Exception {
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
    public void testUuidAsId() throws Exception {
        collection.insertOne(new Document("_id", new UUID(0, 1)));
        collection.insertOne(new Document("_id", new UUID(0, 2)));
        collection.insertOne(new Document("_id", new UUID(999999, 128)));

        assertMongoWriteException(() -> collection.insertOne(new Document("_id", new UUID(0, 1))),
            11000, "DuplicateKey",
            "E11000 duplicate key error collection: testdb.testcoll index: _id_ dup key: { _id: BinData(3, 00000000000000000100000000000000) }");

        assertMongoWriteException(() -> collection.insertOne(new Document("_id", new UUID(999999, 128))),
            11000, "DuplicateKey",
            "E11000 duplicate key error collection: testdb.testcoll index: _id_ dup key: { _id: BinData(3, 3F420F00000000008000000000000000) }");

        collection.deleteOne(new Document("_id", new UUID(0, 2)));

        assertThat(collection.find(json("")))
            .containsExactlyInAnyOrder(
                new Document("_id", new UUID(0, 1)),
                new Document("_id", new UUID(999999, 128))
            );
    }

    @Test
    public void testTypeMatching() throws Exception {
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
            .withMessageContaining("Query failed with error code 9 and error message 'n must match at least one type'");

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(json("'a.b.c': {$type: []}")).first())
            .withMessageContaining("Query failed with error code 9 and error message 'a.b.c must match at least one type'");

        assertThat(collection.find(json("a: {b: {$type: []}}"))).isEmpty();

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(json("n: {$type: 'abc'}")).first())
            .withMessageContaining("Query failed with error code 2 and error message 'Unknown type name alias: abc'");

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(json("n: {$type: null}")).first())
            .withMessageContaining("Query failed with error code 14 and error message 'type must be represented as a number or a string'");

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(json("_id: {$type: 16.3}")).first())
            .withMessageContaining("Query failed with error code 2 and error message 'Invalid numerical type code: 16.3'");
    }

    @Test
    public void testDistinctQueryWithDot() {
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
    public void testDropCollection() throws Exception {
        collection.createIndex(new Document("n", 1));
        collection.createIndex(new Document("b", 1));

        collection.insertOne(json(""));
        assertThat(db.listCollectionNames()).contains(getCollectionName());

        assertThat(collection.listIndexes())
            .containsExactlyInAnyOrder(
                json("key: {_id: 1}").append("ns", collection.getNamespace().getFullName()).append("name", "_id_").append("v", 2),
                json("key: {n: 1}").append("ns", collection.getNamespace().getFullName()).append("name", "n_1").append("v", 2),
                json("key: {b: 1}").append("ns", collection.getNamespace().getFullName()).append("name", "b_1").append("v", 2)
            );

        collection.drop();
        assertThat(db.listCollectionNames()).doesNotContain(getCollectionName());

        assertThat(collection.listIndexes()).isEmpty();
    }

    @Test
    public void testDropCollectionAlsoDropsFromDB() throws Exception {
        collection.insertOne(json(""));
        collection.drop();
        assertThat(collection.countDocuments()).isZero();
        assertThat(db.listCollectionNames()).doesNotContain(getCollectionName());
    }

    @Test
    public void testDropDatabaseAlsoDropsCollectionData() throws Exception {
        collection.insertOne(json(""));
        db.drop();
        assertThat(collection.countDocuments()).isZero();
    }

    @Test
    public void testDropDatabaseDropsAllData() throws Exception {
        collection.insertOne(json("_id: 1"));
        MongoCollection<Document> collection2 = getCollection("testcoll2");
        collection2.insertOne(json("_id: 1"));

        syncClient.dropDatabase(db.getName());
        assertThat(listDatabaseNames()).doesNotContain(db.getName());
        assertThat(collection.countDocuments()).isZero();
        assertThat(db.listCollectionNames()).doesNotContain(getCollectionName(),
            collection2.getNamespace().getCollectionName());

        collection.insertOne(json("_id: 1"));
        collection2.insertOne(json("_id: 1"));
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/107
    @Test
    public void testDropDatabaseAfterAddingIndexMultipleTimes() throws Exception {
        collection.insertOne(json("_id: 1, a: 10"));
        for (int i = 0; i < 3; i++) {
            collection.createIndex(json("a: 1"), new IndexOptions().unique(true));
        }
        syncClient.dropDatabase(db.getName());
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/107
    @Test
    public void testAddIndexAgainWithDifferentOptions() throws Exception {
        collection.insertOne(json("_id: 1, a: 10"));
        collection.createIndex(json("a: 1"), new IndexOptions().unique(true));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.createIndex(json("a: 1"), new IndexOptions().unique(true).sparse(true)))
            .withMessageContaining("Command failed with error 85 (IndexOptionsConflict): 'Index with name: a_1 already exists with different options'");
    }

    @Test
    public void testEmbeddedSort() {
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
    public void testFindAndModifyCommandEmpty() throws Exception {
        Document cmd = new Document("findandmodify", getCollectionName());

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> db.runCommand(cmd))
            .withMessageContaining("Command failed with error 9 (FailedToParse): 'Either an update or remove=true must be specified'");
    }

    @Test
    public void testFindAndModifyCommandIllegalOp() throws Exception {
        collection.insertOne(json("_id: 1"));

        Document cmd = new Document("findAndModify", getCollectionName());
        cmd.put("query", json("_id: 1"));
        cmd.put("update", new Document("$inc", json("_id: 1")));

        assertThat(collection.find().first()).isEqualTo(json("_id: 1"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> db.runCommand(cmd))
            .withMessageContaining("Command failed with error 66 (ImmutableField): 'Performing an update on the path '_id' would modify the immutable field '_id'");
    }

    @Test
    public void testFindAndModifyCommandUpdate() throws Exception {
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
    public void testFindAndModifyCommand_UpdateSameFields() throws Exception {
        collection.insertOne(json("_id: 1"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(json("_id: 1"), json("$inc: {x: 0, a: 1}, $set: {a: 2}")))
            .withMessageContaining("Command failed with error 40 (ConflictingUpdateOperators): 'Updating the path 'a' would create a conflict at 'a'");
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/75
    @Test
    public void testFindAndModifyCommand_UpdateFieldAndItsSubfield() throws Exception {
        collection.insertOne(json("_id: 1, a: {b: {c: 1}}"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(json("_id: 1"), json("$set: {'x': 1, 'a.b': {c: 1}}, $inc: {'a.b.c': 1}")))
            .withMessageContaining("Command failed with error 40 (ConflictingUpdateOperators): 'Updating the path 'a.b.c' would create a conflict at 'a.b'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(json("_id: 1"), json("$set: {'x': 1, 'a.b.c': 1}, $unset: {'a.b': 1}")))
            .withMessageContaining("Command failed with error 40 (ConflictingUpdateOperators): 'Updating the path 'a.b' would create a conflict at 'a.b'");
    }

    @Test
    public void testFindOneAndUpdateError() throws Exception {
        collection.insertOne(json("_id: 1, a: 1"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(json("_id: 1"), json("$inc: {_id: 1}")))
            .withMessageContaining("Command failed with error 66 (ImmutableField): 'Performing an update on the path '_id' would modify the immutable field '_id'");
    }

    @Test
    public void testFindOneAndUpdateFields() throws Exception {
        collection.insertOne(json("_id: 1, a: 1"));
        Document result = collection.findOneAndUpdate(json("_id: 1"), json("$inc: {a: 1}"),
            new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));

        assertThat(result).isEqualTo(json("_id: 1, a: 2"));
    }

    @Test
    public void testFineOneAndUpdateNotFound() throws Exception {
        collection.insertOne(json("_id: 1, a: 1"));
        Document result = collection.findOneAndUpdate(json("_id: 2"), new Document("$inc", json("a: 1")));

        assertThat(result).isNull();
        assertThat(collection.countDocuments()).isEqualTo(1);
    }

    @Test
    public void testFineOneAndUpdateRemove() {
        collection.insertOne(json("_id: 1, a: 1"));
        Document result = collection.findOneAndDelete(json("_id: 1"));

        assertThat(result).isEqualTo(json("_id: 1, a: 1"));
        assertThat(collection.countDocuments()).isZero();
    }

    // https://github.com/foursquare/fongo/issues/32
    @Test
    public void testFineOneAndUpdateReturnNew() {
        collection.insertOne(json("_id: 1, a: 1, b: {c: 1}"));

        Document query = json("_id: 1");
        Document update = json("$inc: {a: 1, 'b.c': 1}");
        Document result = collection.findOneAndUpdate(query, update,
            new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));

        assertThat(result).isEqualTo(json("_id: 1, a: 2, b: {c: 2}"));
    }

    @Test
    public void testFineOneAndUpdateMax() {
        collection.insertOne(json("_id: 1, a: 1, b: {c: 1}"));

        Document query = json("_id: 1");
        Document update = json("$max: {a: 2, 'b.c': 2, d : 'd'}");
        Document result = collection.findOneAndUpdate(query, update,
            new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));

        assertThat(result).isEqualTo(json("_id: 1, a: 2, b: {c: 2}, d : 'd'"));
    }

    @Test
    public void testFineOneAndUpdateMin() {
        collection.insertOne(json("_id: 1, a: 2, b: {c: 1}"));

        Document query = json("_id: 1");
        Document update = json("$min: {a: 1, 'b.c': 2, d : 'd'}");
        Document result = collection.findOneAndUpdate(query, update,
            new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));

        assertThat(result).isEqualTo(json("_id: 1, a: 1, b: {c: 1}, d : 'd'"));
    }

    // https://github.com/foursquare/fongo/issues/32
    @Test
    public void testFindOneAndUpdateReturnOld() {
        collection.insertOne(json("_id: 1, a: 1, b: {c: 1}"));

        Document query = json("_id: 1");
        Document update = json("$inc: {a: 1, 'b.c': 1}");
        Document result = collection.findOneAndUpdate(query, update,
            new FindOneAndUpdateOptions().returnDocument(ReturnDocument.BEFORE));

        assertThat(result).isEqualTo(json("_id: 1, a: 1, b: {c: 1}"));
        assertThat(collection.find(query).first()).isEqualTo(json("_id: 1, a: 2, b: {c: 2}"));
    }

    @Test
    public void testFindOneAndUpdateSorted() throws Exception {
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
    public void testFindOneAndUpdateUpsert() {
        Document result = collection.findOneAndUpdate(json("_id: 1"), json("$inc: {a: 1}"),
            new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));

        assertThat(result).isEqualTo(json("_id: 1, a: 1"));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, a: 1"));
    }

    @Test
    public void testFindOneAndUpdateUpsertReturnBefore() {
        Document result = collection.findOneAndUpdate(json("_id: 1"), json("$inc: {a: 1}"),
            new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.BEFORE));

        assertThat(result).isEqualTo(json(""));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, a: 1"));
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/60
    @Test
    public void testFindOneAndUpdateWithArrayFilters() {
        collection.insertOne(json("_id: 1, grades: [95, 102, 90, 150]"));
        collection.insertOne(json("_id: 2, values: [[1, 2, 3], 'other']"));
        collection.insertOne(json("_id: 3, a: {b: [1, 2, 3]}"));

        collection.findOneAndUpdate(
            json("_id: 1"),
            json("$set: {'grades.$[element]': 'abc'}"),
            new FindOneAndUpdateOptions().arrayFilters(Arrays.asList(json("element: {$gte: 100}"))));

        assertThat(collection.find(json("_id: 1")).first()).isEqualTo(json("_id: 1, grades: [95, 'abc', 90, 'abc']"));

        collection.findOneAndUpdate(
            json("_id: 1"),
            json("$unset: {'grades.$[element]': 1}"),
            new FindOneAndUpdateOptions().arrayFilters(Arrays.asList(json("element: 'abc'"))));

        assertThat(collection.find(json("_id: 1")).first()).isEqualTo(json("_id: 1, grades: [95, null, 90, null]"));

        collection.findOneAndUpdate(
            json("_id: 1"),
            json("$inc: {'grades.$[element]': 1}"),
            new FindOneAndUpdateOptions().arrayFilters(Arrays.asList(json("element: 90"))));

        assertThat(collection.find(json("_id: 1")).first()).isEqualTo(json("_id: 1, grades: [95, null, 91, null]"));

        collection.findOneAndUpdate(
            json("_id: 2"),
            json("$pull: {'values.$[element]': 2}"),
            new FindOneAndUpdateOptions().arrayFilters(Arrays.asList(json("element: {$type: 'array'}"))));

        assertThat(collection.find(json("_id: 2")).first())
            .isEqualTo(json("_id: 2, values: [[1, 3], 'other']"));

        collection.findOneAndUpdate(
            json("_id: 3"),
            json("$mul: {'a.b.$[element]': 10}"),
            new FindOneAndUpdateOptions().arrayFilters(Arrays.asList(json("element: 2"))));

        assertThat(collection.find(json("_id: 3")).first())
            .isEqualTo(json("_id: 3, a: {b: [1, 20, 3]}"));
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/60
    @Test
    public void testUpdateManyWithArrayFilters() {
        collection.insertOne(json("_id: 1, values: [9, 102, 90, 150]"));
        collection.insertOne(json("_id: 2, values: [1, 2, 3, 50]"));

        collection.updateMany(
            json(""),
            json("$set: {'values.$[x]': 20}"),
            new UpdateOptions().arrayFilters(Arrays.asList(json("x: {$gt: 20}")))
        );

        assertThat(collection.find(json("")))
            .containsExactly(
                json("_id: 1, values: [9, 20, 20, 20]"),
                json("_id: 2, values: [1, 2, 3, 20]")
            );
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/60
    @Test
    public void testUpdateOneWithArrayFilter() throws Exception {
        collection.insertOne(json("_id: 1, values: [{name: 'A', active: false}, {name: 'B', active: false}]"));

        collection.updateOne(json("_id: 1"),
            json("$set: {'values.$[elem].active': true}"),
            new UpdateOptions().arrayFilters(Arrays.asList(json("'elem.name': {$in: ['A']}")))
        );

        assertThat(collection.find(json("")))
            .containsExactly(
                json("_id: 1, values: [{name: 'A', active: true}, {name: 'B', active: false}]")
            );
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/60
    @Test
    public void testUpsertWithArrayFilters() {
        collection.updateOne(
            json("_id: 1, values: [0, 1]"),
            json("$set: {'values.$[x]': 20}"),
            new UpdateOptions()
                .upsert(true)
                .arrayFilters(Arrays.asList(json("x: 0")))
        );

        assertThat(collection.find(json("")))
            .containsExactly(
                json("_id: 1, values: [20, 1]")
            );
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/60
    @Test
    public void testUpdateWithMultipleArrayFilters() throws Exception {
        collection.insertOne(json("_id: 1, values: [9, 102, 90, 150]"));
        collection.insertOne(json("_id: 2, values: [1, 2, 30, 50]"));

        collection.updateMany(
            json(""),
            json("$set: {'values.$[tooLow]': 10, 'values.$[tooHigh]': 40}"),
            new UpdateOptions().arrayFilters(Arrays.asList(
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
    public void testUpdateWithMultipleComplexArrayFilters() throws Exception {
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
            new UpdateOptions().arrayFilters(Arrays.asList(
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
    public void testFindOneAndUpdate_IllegalArrayFilters() {
        collection.insertOne(json("_id: 1, grades: 'abc', a: {b: 123}"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(
                json("_id: 1"),
                json("$set: {'grades': 'abc'}"),
                new FindOneAndUpdateOptions().arrayFilters(Arrays.asList(json("element: {$gte: 100}")))))
            .withMessageContaining("Command failed with error 9 (FailedToParse): 'The array filter for identifier 'element' was not used in the update { $set: { grades: \"abc\" } }'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(
                json("_id: 1"),
                json("$set: {'grades': 'abc'}"),
                new FindOneAndUpdateOptions().arrayFilters(Arrays.asList(
                    json("element: {$gte: 100}"),
                    json("element: {$lt: 100}")
                ))))
            .withMessageContaining("Command failed with error 9 (FailedToParse): 'Found multiple array filters with the same top-level field name element'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(
                json("_id: 1"),
                json("$set: {'grades.$[element]': 'abc'}"),
                new FindOneAndUpdateOptions().arrayFilters(Arrays.asList(json("a: {$gte: 100}, b: {$gte: 100}, c: {$gte: 10}")))))
            .withMessageContaining("Command failed with error 9 (FailedToParse): 'Error parsing array filter :: caused by :: Expected a single top-level field name, found 'a' and 'b'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(
                json("_id: 1"),
                json("$set: {'grades.$[element]': 'abc'}"),
                new FindOneAndUpdateOptions().arrayFilters(Arrays.asList(json("")))))
            .withMessageContaining("Command failed with error 9 (FailedToParse): 'Cannot use an expression without a top-level field name in arrayFilters'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(
                json("_id: 1"),
                json("$set: {'grades.$[element]': 'abc'}"),
                new FindOneAndUpdateOptions().arrayFilters(Arrays.asList(json("element: {$gte: 100}")))))
            .withMessageContaining("Command failed with error 2 (BadValue): 'Cannot apply array updates to non-array element grades: \"abc\"'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(
                json("_id: 1"),
                json("$set: {'$[element]': 10}"),
                new FindOneAndUpdateOptions().arrayFilters(Arrays.asList(json("element: 2")))))
            .withMessageContaining("Command failed with error 2 (BadValue): 'Cannot have array filter identifier (i.e. '$[<id>]') element in the first position in path '$[element]'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(
                json("_id: 1"),
                json("$set: {'grades.subGrades.$[element]': 'abc'}"),
                new FindOneAndUpdateOptions().arrayFilters(Arrays.asList(json("element: {$gte: 100}")))))
            .withMessageContaining("Command failed with error 2 (BadValue): 'The path 'grades.subGrades' must exist in the document in order to apply array updates.'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(
                json("_id: 1"),
                json("$set: {'grades.$[some value]': 'abc'}"),
                new FindOneAndUpdateOptions().arrayFilters(Arrays.asList(json("'some value': {$gte: 100}")))))
            .withMessageContaining("Command failed with error 2 (BadValue): 'Error parsing array filter :: caused by :: The top-level field name must be an alphanumeric string beginning with a lowercase letter, found 'some value''");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(
                json("_id: 1"),
                json("$set: {'a.b.$[x]': 'abc'}"),
                new FindOneAndUpdateOptions().arrayFilters(Arrays.asList(json("x: {$gte: 100}")))))
            .withMessageContaining("Command failed with error 2 (BadValue): 'Cannot apply array updates to non-array element b: 123'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(
                json("_id: 1"),
                json("$set: {'grades': 'abc'}"),
                new FindOneAndUpdateOptions().arrayFilters(Arrays.asList(json("'a.b': 10, b: 12")))))
            .withMessageContaining("Command failed with error 9 (FailedToParse): 'Error parsing array filter :: caused by :: Expected a single top-level field name, found 'a' and 'b''");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(
                json("_id: 1"),
                json("$set: {'grades': 'abc'}"),
                new FindOneAndUpdateOptions().arrayFilters(Arrays.asList(
                    json("'a.b': 10"),
                    json("'a.c': 10")
                ))))
            .withMessageContaining("Command failed with error 9 (FailedToParse): 'Found multiple array filters with the same top-level field name a'");
    }

    @Test
    public void testFindOneAndUpdate_IllegalArrayFiltersPaths() {
        collection.insertOne(json("_id: 1, grades: 'abc', a: {b: [1, 2, 3]}"));
        collection.insertOne(json("_id: 2, grades: 'abc', a: {b: [{c: 1}, {c: 2}, {c: 3}]}"));
        collection.insertOne(json("_id: 3, grades: 'abc', a: {b: [[[1, 2], [3, 4]]]}"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(
                json("_id: 1"),
                json("$set: {'a.b.$[x].c': 'abc'}"),
                new FindOneAndUpdateOptions().arrayFilters(Arrays.asList(json("x: {$gt: 1}")))))
            .withMessageContaining("Command failed with error 28 (PathNotViable): 'Cannot create field 'c' in element {1: 2}");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(
                json("_id: 1"),
                json("$set: {'a.b.$[x].c.d': 'abc'}"),
                new FindOneAndUpdateOptions().arrayFilters(Arrays.asList(json("x: {$gt: 1}")))))
            .withMessageContaining("Command failed with error 28 (PathNotViable): 'Cannot create field 'c' in element {1: 2}");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(
                json("_id: 2"),
                json("$set: {'a.b.$[].c.$[]': 'abc'}")))
            .withMessageContaining("Command failed with error 2 (BadValue): 'Cannot apply array updates to non-array element c: 1");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(
                json("_id: 3"),
                json("$set: {'a.b.$[].0.c': 'abc'}")))
            .withMessageContaining("Command failed with error 28 (PathNotViable): 'Cannot create field 'c' in element {0: [ 1, 2 ]}");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(
                json("_id: 3"),
                json("$set: {'a.b.$[].0.$[].c': 'abc'}")))
            .withMessageContaining("Command failed with error 28 (PathNotViable): 'Cannot create field 'c' in element {0: 1}");
    }

    @Test
    public void testFindAndRemoveFromEmbeddedList() {
        collection.insertOne(json("_id: 1, a: [1]"));
        Document result = collection.findOneAndDelete(json("_id: 1"));
        assertThat(result).isEqualTo(json("_id: 1, a: [1]"));
        assertThat(collection.countDocuments()).isZero();
    }

    @Test
    public void testFindOne() {
        collection.insertOne(json("key: 'value'"));
        collection.insertOne(json("key: 'value'"));
        Document result = collection.find().first();
        assertThat(result).isNotNull();
        assertThat(result.get("_id")).isNotNull();
    }

    @Test
    public void testFindOneById() {
        collection.insertOne(json("_id: 1"));
        Document result = collection.find(json("_id: 1")).first();
        assertThat(result).isEqualTo(json("_id: 1"));
        assertThat(collection.find(json("_id: 2")).first()).isNull();
    }

    @Test
    public void testFindOneIn() {
        collection.insertOne(json("_id: 1"));
        Document result = collection.find(json("_id: {$in: [1, 2]}")).first();
        assertThat(result).isEqualTo(json("_id: 1"));
    }

    @Test
    public void testFindWithLimit() {
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
    public void testFindInReverseNaturalOrder() {
        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));

        assertThat(collection.find().sort(json("$natural: -1")))
            .containsExactly(
                json("_id: 2"),
                json("_id: 1")
            );
    }

    @Test
    public void testFindWithPattern() {
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
    public void testFindWithQuery() {
        collection.insertOne(json("name: 'jon'"));
        collection.insertOne(json("name: 'leo'"));
        collection.insertOne(json("name: 'neil'"));
        collection.insertOne(json("name: 'neil'"));

        assertThat(collection.find(json("name: 'neil'"))).hasSize(2);
    }

    @Test
    public void testFindWithSkipLimit() {
        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));
        collection.insertOne(json("_id: 3"));
        collection.insertOne(json("_id: 4"));

        assertThat(collection.find().sort(json("_id: 1")).limit(2).skip(2))
            .containsExactly(json("_id: 3"), json("_id: 4"));
    }

    @Test
    public void testFindWithSkipLimitInReverseOrder() {
        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));
        collection.insertOne(json("_id: 3"));
        collection.insertOne(json("_id: 4"));

        assertThat(collection.find().sort(json("_id: -1")).limit(2).skip(2))
            .containsExactly(json("_id: 2"), json("_id: 1"));
    }

    @Test
    public void testFindWithSkipLimitAfterDelete() {
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
    public void testFullUpdateWithSameId() throws Exception {
        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2, b: 5"));
        collection.insertOne(json("_id: 3"));
        collection.insertOne(json("_id: 4"));

        collection.replaceOne(json("_id: 2, b: 5"), json("_id: 2, a: 5"));

        assertThat(collection.find(json("_id: 2")).first()).isEqualTo(json("_id: 2, a: 5"));
    }

    @Test
    public void testGetCollection() {
        MongoCollection<Document> collection = getCollection("coll");
        getCollection("coll").insertOne(json(""));

        assertThat(collection).isNotNull();
        assertThat(db.listCollectionNames()).contains("coll");
    }

    @Test
    public void testNullId() throws Exception {
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
    public void testIdInQueryResultsInIndexOrder() {
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
    public void testInQuery_Arrays() throws Exception {
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
    public void testIdNotAllowedToBeUpdated() {
        collection.insertOne(json("_id: 1"));

        assertMongoWriteException(() -> collection.replaceOne(json("_id: 1"), json("_id: 2, a: 4")),
            66, "ImmutableField", "After applying the update, the (immutable) field '_id' was found to have been altered to _id: 2");

        // test with $set

        assertMongoWriteException(() -> collection.updateOne(json("_id: 1"), new Document("$set", json("_id: 2"))),
            66, "ImmutableField", "Performing an update on the path '_id' would modify the immutable field '_id'");
    }

    @Test
    public void testIllegalCommand() throws Exception {
        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> db.runCommand(json("foo: 1")))
            .withMessageContaining("Command failed with error 59 (CommandNotFound): 'no such command: 'foo'");
    }

    @Test
    public void testInsert() throws Exception {
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
    public void testInsertDuplicate() throws Exception {
        assertThat(collection.countDocuments()).isEqualTo(0);

        collection.insertOne(json("_id: 1"));
        assertThat(collection.countDocuments()).isEqualTo(1);

        assertMongoWriteException(() -> collection.insertOne(json("_id: 1.0")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: _id_ dup key: { _id: 1.0 }");

        assertThat(collection.countDocuments()).isEqualTo(1);
    }

    @Test
    public void testInsertDuplicateThrows() {
        collection.insertOne(json("_id: 1"));

        assertMongoWriteException(() -> collection.insertOne(json("_id: 1")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: _id_ dup key: { _id: 1 }");
    }

    @Test
    public void testInsertDuplicateWithConcernThrows() {
        collection.insertOne(json("_id: 1"));

        assertMongoWriteException(() -> collection.withWriteConcern(WriteConcern.ACKNOWLEDGED).insertOne(json("_id: 1")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: _id_ dup key: { _id: 1 }");
    }

    @Test
    public void testInsertIncrementsCount() {
        assertThat(collection.countDocuments()).isZero();
        collection.insertOne(json("key: 'value'"));
        assertThat(collection.countDocuments()).isEqualTo(1);
    }

    @Test
    public void testInsertQuery() throws Exception {
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
    public void testInsertRemove() throws Exception {
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
    public void testInsertInSystemNamespace() throws Exception {
        assertMongoWriteException(() -> getCollection("system.foobar").insertOne(json("")),
            16459, "attempt to insert in system namespace");

        assertMongoWriteException(() -> getCollection("system.namespaces").insertOne(json("")),
            16459, "attempt to insert in system namespace");
    }

    @Test
    public void testListDatabaseNames() throws Exception {
        assertThat(listDatabaseNames()).isEmpty();
        collection.insertOne(json(""));
        assertThat(listDatabaseNames()).containsExactly(db.getName());
        getDatabase().getCollection("some-collection").insertOne(json(""));
        assertThat(listDatabaseNames()).containsExactly("bar", db.getName());
    }

    private MongoDatabase getDatabase() {
        return syncClient.getDatabase(OTHER_TEST_DATABASE_NAME);
    }

    private List<String> listDatabaseNames() {
        List<String> databaseNames = new ArrayList<>();
        for (String databaseName : syncClient.listDatabaseNames()) {
            databaseNames.add(databaseName);
        }
        return databaseNames;
    }

    @Test
    public void testQuery() throws Exception {
        Document obj = collection.find(json("_id: 1")).first();
        assertThat(obj).isNull();
        assertThat(collection.countDocuments()).isEqualTo(0);
    }

    @Test
    public void testQueryAll() throws Exception {
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
    public void testQueryCount() throws Exception {
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
    public void testQueryLimitEmptyQuery() throws Exception {
        for (int i = 0; i < 5; i++) {
            collection.insertOne(json(""));
        }
        assertThat(collection.countDocuments(json(""), new CountOptions().limit(1))).isEqualTo(1);
        assertThat(collection.countDocuments(json(""), new CountOptions().limit(-1))).isEqualTo(5);
        assertThat(collection.countDocuments(json(""))).isEqualTo(5);
    }

    @Test
    public void testQueryLimitSimpleQuery() throws Exception {
        for (int i = 0; i < 5; i++) {
            collection.insertOne(json("a: 1"));
        }
        assertThat(collection.countDocuments(json("a: 1"), new CountOptions().limit(1))).isEqualTo(1);
        assertThat(collection.countDocuments(json("a: 1"), new CountOptions().limit(-1))).isEqualTo(5);
        assertThat(collection.countDocuments(json("a: 1"))).isEqualTo(5);
    }

    @Test
    public void testQueryNull() throws Exception {
        Document object = json("_id: 1");
        collection.insertOne(object);
        assertThat(collection.find(json("foo: null")).first()).isEqualTo(object);
    }

    @Test
    public void testQuerySkipLimitEmptyQuery() throws Exception {
        assertThat(collection.countDocuments(json(""), new CountOptions().skip(3))).isEqualTo(0);

        for (int i = 0; i < 10; i++) {
            collection.insertOne(json(""));
        }

        assertThat(collection.countDocuments(json(""), new CountOptions().skip(3))).isEqualTo(7);
        assertThat(collection.countDocuments(json(""), new CountOptions().skip(15))).isEqualTo(0);
        assertThat(collection.countDocuments(json(""), new CountOptions().skip(3).limit(5))).isEqualTo(5);
    }

    @Test
    public void testQuerySkipLimitSimpleQuery() throws Exception {
        assertThat(collection.countDocuments(json("a: 1"), new CountOptions().skip(3))).isEqualTo(0);

        for (int i = 0; i < 10; i++) {
            collection.insertOne(json("a: 1"));
        }

        assertThat(collection.countDocuments(json("a: 1"), new CountOptions().skip(3))).isEqualTo(7);
        assertThat(collection.countDocuments(json("a: 1"), new CountOptions().skip(3).limit(5))).isEqualTo(5);
        assertThat(collection.countDocuments(json("a: 1"), new CountOptions().skip(15).limit(5))).isEqualTo(0);
    }

    @Test
    public void testQuerySort() throws Exception {
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
    public void testQueryWithFieldSelector() throws Exception {
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
    public void testQueryWithDotNotationFieldSelector() throws Exception {
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

        obj = collection.find(json("_id: 1")).projection(json("'foo..': 1")).first();
        assertThat(obj).isEqualTo(json("_id: 1, foo: {}"));

        obj = collection.find(json("_id: 2")).projection(json("'foo.a.b': 1, 'foo.b': 1, 'foo.c': 1, 'foo.c.d': 1")).first();
        assertThat(obj).isEqualTo(json("_id: 2, foo: {b: null}"));

        obj = collection.find(json("_id: 2")).projection(json("'foo.a': 1")).first();
        assertThat(obj).isEqualTo(json("_id: 2, foo: {a: null}"));

        obj = collection.find(json("_id: 2")).projection(json("'foo.c': 1")).first();
        assertThat(obj).isEqualTo(json("_id: 2, foo: {}"));
    }

    @Test
    public void testQueryWithDotNotationFieldSelector_Array() throws Exception {
        collection.insertOne(json("_id: 1, values: [1, 2, {x: 100, y: 10}, {x: 200}]"));

        Document obj = collection.find(json("_id: 1")).projection(json("'values.0': 1, 'values.x': 1")).first();
        assertThat(obj).isEqualTo(json("_id: 1, values: [{x: 100}, {x: 200}]"));

        obj = collection.find(json("_id: 1")).projection(json("'values.y': 1")).first();
        assertThat(obj).isEqualTo(json("_id: 1, values: [{y: 10}, {}]"));

        obj = collection.find(json("_id: 1")).projection(json("'values.x': 1, 'values.y': 1")).first();
        assertThat(obj).isEqualTo(json("_id: 1, values: [{x: 100, y: 10}, {x: 200}]"));
    }

    @Test
    public void testQueryWithIllegalFieldSelection() throws Exception {
        collection.insertOne(json("_id: 1"));

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(json("_id: 1")).projection(json("values: {x: 1, y: 1}")).first())
            .withMessageContaining("Query failed with error code 2 and error message '>1 field in obj: { x: 1, y: 1 }'");
    }

    @Test
    public void testQuerySystemNamespace() throws Exception {
        assertThat(getCollection("system.foobar").find().first()).isNull();
        assertThat(db.listCollectionNames()).isEmpty();

        collection.insertOne(json(""));
        Document expectedObj = new Document("name", collection.getNamespace().getFullName());
        Document coll = getCollection("system.namespaces").find(expectedObj).first();
        assertThat(coll).isEqualTo(expectedObj);
    }

    @Test
    public void testQueryAllExpression() throws Exception {
        collection.insertOne(json("a: [{x: 1}, {x: 2}]"));
        collection.insertOne(json("a: [{x: 2}, {x: 3}]"));

        assertThat(collection.countDocuments(json("'a.x': {$all: [1, 2]}"))).isEqualTo(1);
        assertThat(collection.countDocuments(json("'a.x': {$all: [2, 3]}"))).isEqualTo(1);
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/36
    @Test
    public void testAndQueryWithAllAndNin() throws Exception {
        collection.insertOne(json("_id: 1, tags: ['A', 'B']"));
        collection.insertOne(json("_id: 2, tags: ['A', 'D']"));
        collection.insertOne(json("_id: 3, tags: ['A', 'C']"));
        collection.insertOne(json("_id: 4, tags: ['C', 'D']"));

        assertThat(collection.find(json("$and: [{'tags': {$all: ['A']}}, {'tags': {$nin: ['B', 'C']}}]")))
            .containsExactly(json("_id: 2, tags: ['A', 'D']"));
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/96
    @Test
    public void testAndQueryWithAllAndSize() throws Exception {
        collection.insertOne(json("_id: 1, list: ['A', 'B']"));
        collection.insertOne(json("_id: 2, list: ['A', 'B', 'C']"));

        assertThat(collection.find(json("$and: [{list: {$size: 2}}, {list: {$all: ['A', 'B']}}]}")))
            .containsExactly(json("_id: 1, list: ['A', 'B']"));

        assertThat(collection.find(json("list: {$all: ['A', 'B'], $size: 2}")))
            .containsExactly(json("_id: 1, list: ['A', 'B']"));
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/36
    @Test
    public void testMatchesAllWithEmptyCollection() throws Exception {
        collection.insertOne(json("_id: 1, text: 'TextA', tags: []"));
        collection.insertOne(json("_id: 2, text: 'TextB', tags: []"));
        collection.insertOne(json("_id: 3, text: 'TextA', tags: ['A']"));

        assertThat(collection.find(json("$and: [{'text': 'TextA'}, {'tags': {$all: []}}]"))).isEmpty();
    }

    @Test
    public void testQueryWithSubdocumentIndex() throws Exception {
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
    public void testQueryBinaryData() throws Exception {
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
    public void testRemove() {
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
    public void testRemoveSingle() throws Exception {
        Document obj = new Document("_id", ObjectId.get());
        collection.insertOne(obj);
        collection.deleteOne(obj);
    }

    @Test
    public void testRemoveReturnsModifiedDocumentCount() {
        collection.insertOne(json(""));
        collection.insertOne(json(""));

        DeleteResult result = collection.deleteMany(json(""));
        assertThat(result.getDeletedCount()).isEqualTo(2);

        result = collection.deleteMany(json(""));
        assertThat(result.getDeletedCount()).isEqualTo(0);
    }

    @Test
    public void testReservedCollectionNames() throws Exception {
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
    public void testServerStatus() throws Exception {
        Document serverStatus = runCommand("serverStatus");
        assertThat(serverStatus.getDouble("ok")).isEqualTo(1);
        assertThat(serverStatus.get("uptime")).isInstanceOf(Number.class);
        assertThat(serverStatus.get("uptimeMillis")).isInstanceOf(Long.class);
        Instant serverTime = ((Date) serverStatus.get("localTime")).toInstant();
        assertThat(serverTime).isEqualTo(TEST_CLOCK.instant());

        Document connections = (Document) serverStatus.get("connections");
        assertThat(connections.get("current")).isNotNull();
    }

    @Test
    public void testPing() throws Exception {
        assertThat(runCommand("ping").getDouble("ok")).isEqualTo(1.0);
        assertThat(runCommand(json("ping: true")).getDouble("ok")).isEqualTo(1.0);
        assertThat(runCommand(json("ping: 2.0")).getDouble("ok")).isEqualTo(1.0);
    }

    @Test
    public void testReplSetGetStatus() throws Exception {
        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> runCommand("replSetGetStatus"))
            .withMessageContaining("Command failed with error 76 (NoReplicationEnabled): 'not running with --replSet'");
    }

    @Test
    public void testWhatsMyUri() throws Exception {
        for (String dbName : new String[] { "admin", "local", "test" }) {
            Document result = syncClient.getDatabase(dbName).runCommand(new Document("whatsmyuri", 1));
            assertThat(result.get("you")).isNotNull();
            assertThat(result.get("you").toString()).matches("\\d+\\.\\d+\\.\\d+\\.\\d+:\\d*");
        }
    }

    @Test
    public void testSortDocuments() throws Exception {
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
    public void testSortByEmbeddedKey() {
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
    public void testUpdate() throws Exception {
        Document object = json("_id: 1");
        Document newObject = json("_id: 1, foo: 'bar'");

        collection.insertOne(object);
        UpdateResult result = collection.replaceOne(object, newObject);
        assertThat(result.getModifiedCount()).isEqualTo(1);
        assertThat(result.getUpsertedId()).isNull();
        assertThat(collection.find(object).first()).isEqualTo(newObject);
    }

    @Test
    public void testUpdateNothing() throws Exception {
        Document object = json("_id: 1");
        UpdateResult result = collection.replaceOne(object, object);
        assertThat(result.getModifiedCount()).isEqualTo(0);
        assertThat(result.getMatchedCount()).isEqualTo(0);
        assertThat(result.getUpsertedId()).isNull();
    }

    @Test
    public void testUpdateBlank() throws Exception {
        Document document = json("'': 1, _id: 2, a: 3, b: 4");
        collection.insertOne(document);

        collection.updateOne(json(""), json("$set: {c: 5}"));
        assertThat(collection.find().first()).isEqualTo(json("'': 1, _id: 2, a: 3, b: 4, c: 5"));
    }

    @Test
    public void testUpdateEmptyPositional() throws Exception {
        collection.insertOne(json(""));
        assertMongoWriteException(() -> collection.updateOne(json(""), json("$set: {'a.$.b': 1}")),
            2, "BadValue", "The positional operator did not find the match needed from the query.");
    }

    @Test
    public void testUpdateMultiplePositional() throws Exception {
        collection.insertOne(json("a: {b: {c: 1}}"));
        assertMongoWriteException(() -> collection.updateOne(json("'a.b.c': 1"), json("$set: {'a.$.b.$.c': 1}")),
            2, "BadValue", "Too many positional (i.e. '$') elements found in path 'a.$.b.$.c'");
    }

    @Test
    public void testUpdateIllegalFieldName() throws Exception {
        // Disallow $ in field names - SERVER-3730

        collection.insertOne(json("x: 1"));

        collection.updateOne(json("x: 1"), json("$set: {y: 1}")); // ok

        assertMongoWriteException(() -> collection.updateOne(json("x: 1"), json("$set: {$z: 1}")),
            52, "DollarPrefixedFieldName", "The dollar ($) prefixed field '$z' in '$z' is not valid for storage.");

        // unset ok to remove bad fields
        collection.updateOne(json("x: 1"), json("$unset: {$z: 1}"));

        assertMongoWriteException(() -> collection.updateOne(json("x: 1"), json("$inc: {$z: 1}")),
            52, "DollarPrefixedFieldName", "The dollar ($) prefixed field '$z' in '$z' is not valid for storage.");

        assertMongoWriteException(() -> collection.updateOne(json("x: 1"), json("$push: {$z: [1, 2, 3]}")),
            52, "DollarPrefixedFieldName", "The dollar ($) prefixed field '$z' in '$z' is not valid for storage.");
    }

    @Test
    public void testUpdateSubdocument() throws Exception {
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> collection.updateOne(json(""), json("'a.b.c': 123")))
            .withMessage("Invalid BSON field name a.b.c");
    }

    @Test
    public void testInsertWithIllegalFieldNames() throws Exception {
        for (String illegalFieldName : Arrays.asList("a.", "a.b.", "a.....111", "a.b")) {
            assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> collection.insertOne(new Document(illegalFieldName, 1)))
                .withMessage("Invalid BSON field name " + illegalFieldName);
        }
    }

    @Test
    public void testUpdateIdNoChange() {
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
    public void testUpdatePush() throws Exception {
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
        expected.put("field1", Arrays.asList("value", "value"));
        assertThat(collection.find(idObj).first()).isEqualTo(expected);
    }

    @Test
    public void testUpdatePushEach() throws Exception {
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
    public void testUpdatePushSlice() throws Exception {
        collection.insertOne(json("_id: 1"));

        collection.updateOne(json(""), json("$push: {value: {$each: ['a', 'b', 'c'], $slice: 4}}"));
        collection.updateOne(json(""), json("$push: {value: {$each: [1, 2, 3], $slice: 4}}"));

        assertThat(collection.find())
            .containsExactly(json("_id: 1, value: ['a', 'b', 'c', 1]"));
    }

    @Test
    public void testUpdatePushSort() throws Exception {
        collection.insertOne(json("_id: 1"));

        collection.updateOne(json(""), json("$push: {value: {$each: [1, 5, 6, 3], $sort: -1}}"));
        assertThat(collection.find()).containsExactly(json("_id: 1, value: [6, 5, 3, 1]"));

        collection.updateOne(json(""), json("$push: {value: {$each: [{value: 1}, {value: 3}, {value: 2}], $sort: {value: 1}}}"));
        assertThat(collection.find()).containsExactly(json("_id: 1, value: [6, 5, 3, 1, {value: 1}, {value: 2}, {value: 3}]"));
    }

    @Test
    public void testUpdatePushSortAndSlice() throws Exception {
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
    public void testUpdatePushPosition() throws Exception {
        collection.insertOne(json("_id: 1, value: [1, 2]"));

        collection.updateOne(json(""), json("$push: {value: {$each: [3, 4], $position: 10}}"));
        assertThat(collection.find()).containsExactly(json("_id: 1, value: [1, 2, 3, 4]"));

        collection.updateOne(json(""), json("$push: {value: {$each: ['x'], $position: 2}}"));
        assertThat(collection.find()).containsExactly(json("_id: 1, value: [1, 2, 'x', 3, 4]"));

        collection.updateOne(json(""), json("$push: {value: {$each: ['y'], $position: -2}}"));
        assertThat(collection.find()).containsExactly(json("_id: 1, value: [1, 2, 'x', 'y', 3, 4]"));
    }

    @Test
    public void testUpdatePushEach_unknownModifier() throws Exception {
        collection.insertOne(json("_id: 1"));

        assertMongoWriteException(() -> collection.updateOne(json(""), json("$push: {value: {$each: [1, 2, 3], $illegal: 1}}")),
            2, "BadValue", "Unrecognized clause in $push: $illegal");

        assertMongoWriteException(() -> collection.updateOne(json(""), json("$push: {value: {$position: 1}}")),
            52, "DollarPrefixedFieldName", "The dollar ($) prefixed field '$position' in 'value..$position' is not valid for storage.");

        assertMongoWriteException(() -> collection.updateOne(json(""), json("$push: {value: {$illegal: 1}}")),
            52, "DollarPrefixedFieldName", "The dollar ($) prefixed field '$illegal' in 'value..$illegal' is not valid for storage.");
    }

    @Test
    public void testUpdatePushEach_illegalOptions() throws Exception {
        collection.insertOne(json("_id: 1"));

        assertMongoWriteException(() -> collection.updateOne(json(""), json("$push: {value: {$each: [1, 2, 3], $slice: 'abc'}}")),
            2, "BadValue", "The value for $slice must be an integer value but was given type: string");

        assertMongoWriteException(() -> collection.updateOne(json(""), json("$push: {value: {$each: [1, 2, 3], $sort: 'abc'}}")),
            2, "BadValue", "The $sort is invalid: use 1/-1 to sort the whole element, or {field:1/-1} to sort embedded fields");

        assertMongoWriteException(() -> collection.updateOne(json(""), json("$push: {value: {$each: [1, 2, 3], $position: 'abc'}}")),
            2, "BadValue", "The value for $position must be an integer value, not of type: string");
    }

    @Test
    public void testUpdatePushAll() throws Exception {
        collection.insertOne(json("_id: 1"));

        assertMongoWriteException(() -> collection.updateOne(json(""), json("$pushAll: {field: 'value'}")),
            9, "FailedToParse", "Unknown modifier: $pushAll. Expected a valid update modifier or pipeline-style update specified as an array");
    }

    @Test
    public void testUpdateAddToSet() throws Exception {
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
    public void testUpdateAddToSetEach() throws Exception {
        collection.insertOne(json("_id: 1"));

        collection.updateOne(json("_id: 1"), addEachToSet("a", Arrays.asList(6, 5, 4)));
        assertThat(collection.find()).containsExactly(json("_id: 1, a: [6, 5, 4]"));

        collection.updateOne(json("_id: 1"), addEachToSet("a", Arrays.asList(3, 2, 1)));
        assertThat(collection.find()).containsExactly(json("_id: 1, a: [6, 5, 4, 3, 2, 1]"));

        collection.updateOne(json("_id: 1"), addEachToSet("a", Arrays.asList(7, 7, 9, 2)));
        assertThat(collection.find()).containsExactly(json("_id: 1, a: [6, 5, 4, 3, 2, 1, 7, 9]"));

        collection.updateOne(json("_id: 1"), addEachToSet("a", Arrays.asList(12, 13, 12)));
        assertThat(collection.find()).containsExactly(json("_id: 1, a: [6, 5, 4, 3, 2, 1, 7, 9, 12, 13]"));

        collection.replaceOne(json("_id: 1"), json("_id: 1"));

        collection.updateOne(json("_id: 1"), json("$addToSet: {value: {key: 'x'}}"));
        assertThat(collection.find()).containsExactly(json("_id: 1, value: [{key: 'x'}]"));
    }

    @Test
    public void testUpdateAddToSetEach_unknownModifier() throws Exception {
        collection.insertOne(json("_id: 1"));

        assertMongoWriteException(() -> collection.updateOne(json("_id: 1"), json("$addToSet: {value: {$each: [1, 2, 3], $slice: 2}}")),
            2, "BadValue", "Found unexpected fields after $each in $addToSet: { $each: [ 1, 2, 3 ], $slice: 2 }");

        assertMongoWriteException(() -> collection.updateOne(json("_id: 1"), json("$addToSet: {value: {$each: [1, 2, 3], value: 2}}")),
            2, "BadValue", "Found unexpected fields after $each in $addToSet: { $each: [ 1, 2, 3 ], value: 2 }");

        String expectedPathPrefix = getExpectedPathPrefix_testUpdateAddToSetEach_unknownModifier();

        assertMongoWriteException(() -> collection.updateOne(json("_id: 1"), json("$addToSet: {value: {key: 2, $each: [1, 2, 3]}}")),
            52, "DollarPrefixedFieldName", "The dollar ($) prefixed field '$each' in '" + expectedPathPrefix + ".$each' is not valid for storage.");

        assertMongoWriteException(() -> collection.updateOne(json("_id: 1"), json("$addToSet: {value: {$slice: 2, $each: [1, 2, 3]}}")),
            52, "DollarPrefixedFieldName", "The dollar ($) prefixed field '$slice' in '" + expectedPathPrefix + ".$slice' is not valid for storage.");
    }

    protected String getExpectedPathPrefix_testUpdateAddToSetEach_unknownModifier() {
        return "value.";
    }

    @Test
    public void testUpdateDatasize() throws Exception {
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
    public void testUpdatePull() throws Exception {
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

        assertThat(collection.find(obj).first().get("field")).isEqualTo(Collections.singletonList("value2"));

        // pull with multiple fields

        collection.updateOne(obj, json("$set: {field1: ['value1', 'value2', 'value1']}"));
        collection.updateOne(obj, json("$set: {field2: ['value3', 'value3', 'value1']}"));

        collection.updateOne(obj, json("$pull: {field1: 'value2', field2: 'value3'}"));

        assertThat(collection.find(obj).first().get("field1")).isEqualTo(Arrays.asList("value1", "value1"));
        assertThat(collection.find(obj).first().get("field2")).isEqualTo(Collections.singletonList("value1"));
    }

    @Test
    public void testUpdatePullValueWithCondition() {
        collection.insertOne(json("_id: 1, votes: [ 3, 5, 6, 7, 7, 8 ]"));
        collection.updateOne(json("_id: 1"), json("$pull: {votes: {$gte: 6}}"));

        assertThat(collection.find().first()).isEqualTo(json("_id: 1, votes: [3, 5]"));
    }

    @Test
    public void testUpdatePullDocuments() {
        collection.insertOne(json("_id: 1, results: [{item: 'A', score: 5}, {item: 'B', score: 8, comment: 'foobar'}]"));
        collection.insertOne(json("_id: 2, results: [{item: 'C', score: 8, comment: 'foobar'}, {item: 'B', score: 4}]"));

        collection.updateOne(json(""), json("$pull: {results: {score: 8 , item: 'B'}}"));

        assertThat(collection.find(json("_id: 1")).first()).isEqualTo(json("_id: 1, results: [{item: 'A', score: 5}]"));
        assertThat(collection.find(json("_id: 2")).first()).isEqualTo(json("_id: 2, results: [{item: 'C', score: 8, comment: 'foobar'}, {item: 'B', score: 4}]"));
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/20
    @Test
    public void testUpdatePullLeavesEmptyArray() {
        Document obj = json("_id: 1");
        collection.insertOne(obj);
        collection.updateOne(obj, json("$set: {field: [{'key1': 'value1', 'key2': 'value2'}]}"));
        collection.updateOne(obj, json("$pull: {field: {'key1': 'value1'}}"));

        assertThat(collection.find(obj).first()).isEqualTo(json("_id: 1, field: []"));
    }

    @Test
    public void testUpdatePullAll() throws Exception {
        Document obj = json("_id: 1");
        collection.insertOne(obj);
        collection.updateOne(obj, json("$set: {field: 'value'}"));
        assertMongoWriteException(() -> collection.updateOne(obj, json("$pullAll: {field: 'value'}")),
            2, "BadValue", "$pullAll requires an array argument but was given a string");

        collection.updateOne(obj, json("$set: {field1: ['value1', 'value2', 'value1', 'value3', 'value4', 'value3']}"));

        collection.updateOne(obj, json("$pullAll: {field1: ['value1', 'value3']}"));

        assertThat(collection.find(obj).first().get("field1")).isEqualTo(Arrays.asList("value2", "value4"));

        assertMongoWriteException(() -> collection.updateOne(obj, json("$pullAll: {field1: 'bar'}")),
            2, "BadValue", "$pullAll requires an array argument but was given a string");
    }

    @Test
    public void testUpdatePullAll_Documents() throws Exception {
        collection.insertOne(json("_id: 1, persons: [{id: 1}, {id: 2}, {id: 5}, {id: 5}, {id: 1}, {id: 0}]"));

        collection.updateOne(json("_id: 1"), json("$pullAll: {persons: [{id: 0.0}, {id: 5}]}"));

        assertThat(collection.find(json("")))
            .containsExactly(json("_id: 1, persons: [{id: 1}, {id: 2}, {id: 1}]"));
    }

    @Test
    public void testUpdateSet() throws Exception {
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

        collection.updateOne(object, json("$set: {'other': null}"));
        expected.putAll(json("other: null"));
        assertThat(collection.find(object).first()).isEqualTo(expected);
    }

    @Test
    public void testUpdateSet_arrayOfDocuments() throws Exception {
        collection.insertOne(json("_id: 1, foo: [{bar: 1}, {bar: 2}]"));

        assertMongoWriteException(() -> collection.updateOne(json("_id: 1"), json("$set: {'foo.bar': 3}")),
            28, "PathNotViable", "Cannot create field 'bar' in element {foo: [ { bar: 1 }, { bar: 2 } ]}");
    }

    @Test
    public void testUpdateSetOnInsert() throws Exception {
        Document object = json("_id: 1");
        collection.updateOne(object, json("$set: {b: 3}, $setOnInsert: {a: 3}"), new UpdateOptions().upsert(true));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, b: 3, a: 3"));

        collection.updateOne(object, json("$set: {b: 4}, $setOnInsert: {a: 5}"), new UpdateOptions().upsert(true));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, b: 4, a: 3")); // 'a' is unchanged
    }

    @Test
    public void testUpdateSetWithArrayIndices() throws Exception {

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
    public void testUpdateUnsetWithArrayIndices() throws Exception {

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
    public void testUpdateMax() throws Exception {
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
    public void testUpdateMin() throws Exception {
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
    public void testUpdateMinMaxWithLists() throws Exception {
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
    public void testUpdateMaxCompareNumbers() throws Exception {
        Document object = json("_id: 1, highScore: 800, lowScore: 200");

        collection.insertOne(object);

        collection.updateOne(json("_id: 1"), json("$max: {highScore: 950}"));
        assertThat(collection.find(json("_id: 1")).first()).isEqualTo(json("_id: 1, highScore: 950, lowScore: 200"));

        collection.updateOne(json("_id: 1"), json("$max: {highScore: 870}"));
        assertThat(collection.find(json("_id: 1")).first()).isEqualTo(json("_id: 1, highScore: 950, lowScore: 200"));
    }

    // see http://docs.mongodb.org/manual/reference/operator/update/max
    @Test
    public void testUpdateMaxCompareDates() throws Exception {
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
    public void testUpdateMinCompareNumbers() throws Exception {
        Document object = json("_id: 1, highScore: 800, lowScore: 200");

        collection.insertOne(object);

        collection.updateOne(json("_id: 1"), json("$min: {lowScore: 150}"));
        assertThat(collection.find(json("_id: 1")).first()).isEqualTo(json("_id: 1, highScore: 800, lowScore: 150"));

        collection.updateOne(json("_id: 1"), json("$min: {lowScore: 250}"));
        assertThat(collection.find(json("_id: 1")).first()).isEqualTo(json("_id: 1, highScore: 800, lowScore: 150"));
    }

    // see http://docs.mongodb.org/manual/reference/operator/update/min
    @Test
    public void testUpdateMinCompareDates() throws Exception {
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
    public void testUpdatePop() throws Exception {
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
    public void testUpdateUnset() throws Exception {
        Document obj = json("_id: 1, a: 1, b: null, c: 'value'");
        collection.insertOne(obj);
        assertMongoWriteException(() -> collection.updateOne(obj, json("$unset: {_id: ''}")),
            66, "ImmutableField", "Performing an update on the path '_id' would modify the immutable field '_id'");

        collection.updateOne(json("_id: 1"), json("$unset: {'a.b.z':1}"));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, a: 1, b: null, c: 'value'"));

        collection.updateOne(obj, json("$unset: {a:'', b:''}"));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, c: 'value'"));

        collection.updateOne(obj, Updates.unset("c.y"));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, c: 'value'"));

        collection.replaceOne(json("_id: 1"), json("a: {b: 'foo', c: 'bar'}"));

        collection.updateOne(json("_id: 1"), json("$unset: {'a.b':1}"));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, a: {c: 'bar'}"));

        collection.updateOne(json("_id: 1"), json("$unset: {'a.b.z':1}"));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, a: {c: 'bar'}"));
    }

    @Test
    public void testUpdateWithIdIn() {
        collection.insertOne(json("_id: 1"));
        Document update = json("$push: {n: {_id: 2, u: 3}}, $inc: {c: 4}");
        Document expected = json("_id: 1, n: [{_id: 2, u: 3}], c: 4");
        collection.updateOne(json("_id: {$in: [1]}"), update);
        assertThat(collection.find().first()).isEqualTo(expected);
    }

    @Test
    public void testUpdateMulti() throws Exception {
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
    public void testUpdateIllegalInt() throws Exception {
        collection.insertOne(json("_id: 1, a: {x: 1}"));

        assertMongoWriteException(() -> collection.updateOne(json("_id: 1"), json("$inc: {a: 1}")),
            14, "TypeMismatch", "Cannot apply $inc to a value of non-numeric type. {_id: 1} has the field 'a' of non-numeric type object");

        assertThatExceptionOfType(MongoServerException.class)
            .isThrownBy(() -> collection.updateOne(json("_id: 1"), json("$inc: {'a.x': 'b'}")))
            .withMessage("Cannot increment with non-numeric argument: {a.x: \"b\"}");
    }

    @Test
    public void testUpdateWithIdInMulti() {
        collection.insertMany(Arrays.asList(json("_id: 1"), json("_id: 2")));
        collection.updateMany(json("_id: {$in: [1, 2]}"), json("$set: {n: 1}"));
        assertThat(collection.find())
            .containsExactly(
                json("_id: 1, n: 1"),
                json("_id: 2, n: 1")
            );
    }

    @Test
    public void testUpdateWithIdInMultiReturnModifiedDocumentCount() {
        collection.insertMany(Arrays.asList(json("_id: 1"), json("_id: 2")));
        UpdateResult result = collection.updateMany(json("_id: {$in: [1, 2]}"), json("$set: {n: 1}"));
        assertThat(result.getModifiedCount()).isEqualTo(2);
    }

    @Test
    public void testUpdateWithIdQuery() {
        collection.insertMany(Arrays.asList(json("_id: 1"), json("_id: 2")));
        collection.updateMany(json("_id: {$gt:1}"), json("$set: {n: 1}"));
        assertThat(collection.find())
            .containsExactly(json("_id: 1"), json("_id: 2, n: 1"));
    }

    @Test
    public void testUpdateWithObjectId() {
        collection.insertOne(json("_id: {n: 1}"));
        UpdateResult result = collection.updateOne(json("_id: {n: 1}"), json("$set: {a: 1}"));
        assertThat(result.getModifiedCount()).isEqualTo(1);
        assertThat(collection.find().first()).isEqualTo(json("_id: {n: 1}, a: 1"));
    }

    @Test
    public void testUpdateArrayMatch() throws Exception {

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
    public void testUpdateArrayMatch_MultipleFields() throws Exception {
        collection.insertOne(json("_id: 1, a: [{x: 1, y: 1}, {x: 2, y: 2}, {x: 3, y: 3}]"));

        collection.updateOne(json("'a.x': 2"),
            json("$inc: {'a.$.y': 1, 'a.$.x': 1}, $set: {'a.$.foo': 1, 'a.$.foo2': 1}"));

        assertThat(collection.find(json("")))
            .containsExactly(json("_id: 1, a: [{x: 1, y: 1}, {x: 3, y: 3, foo: 1, foo2: 1}, {x: 3, y: 3}]"));
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/113
    @Test
    public void testUpdateArrayMatch_updateMany() throws Exception {
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

    // https://github.com/bwaldvogel/mongo-java-server/issues/32
    @Test
    public void testUpdateWithNotAndSizeOperator() throws Exception {
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
    public void testMultiUpdateArrayMatch() throws Exception {
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
    public void testUpsert() {
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
    public void testUpsertFieldOrder() throws Exception {
        collection.updateOne(json("'x.y': 2"), json("$inc: {a: 7}"), new UpdateOptions().upsert(true));
        Document obj = collection.find().first();
        obj.remove("_id");
        // this actually differs from the official MongoDB implementation
        assertThat(obj).isEqualTo(json("x: {y: 2}, a: 7"));
    }

    @Test
    public void testUpsertWithoutId() {
        UpdateResult result = collection.updateOne(eq("a", 1), set("a", 2), new UpdateOptions().upsert(true));
        assertThat(result.getModifiedCount()).isEqualTo(0);
        assertThat(result.getUpsertedId()).isNotNull();
        assertThat(collection.find().first().get("_id")).isInstanceOf(ObjectId.class);
        assertThat(collection.find().first().get("a")).isEqualTo(2);
    }

    @Test
    public void testUpsertOnIdWithPush() {
        Document update1 = json("$push: {c: {a: 1, b: 2}}");
        Document update2 = json("$push: {c: {a: 3, b: 4}}");

        collection.updateOne(json("_id: 1"), update1, new UpdateOptions().upsert(true));

        collection.updateOne(json("_id: 1"), update2, new UpdateOptions().upsert(true));

        Document expected = json("_id: 1, c: [{a: 1, b: 2}, {a: 3, b: 4}]");

        assertThat(collection.find(json("'c.a':3, 'c.b':4")).first()).isEqualTo(expected);
    }

    @Test
    public void testUpsertWithConditional() {
        Document query = json("_id: 1, b: {$gt: 5}");
        Document update = json("$inc: {a: 1}");
        UpdateResult updateResult = collection.updateOne(query, update, new UpdateOptions().upsert(true));
        assertThat(updateResult.getModifiedCount()).isZero();
        assertThat(updateResult.getMatchedCount()).isZero();
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, a: 1"));
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/29
    @Test
    public void testUpsertWithoutChange() {
        collection.insertOne(json("_id: 1, a: 2, b: 3"));
        Document query = json("_id: 1");
        Document update = json("$set: {a: 2}");
        UpdateResult updateResult = collection.updateOne(query, update, new UpdateOptions().upsert(true));
        assertThat(updateResult.getModifiedCount()).isZero();
        assertThat(updateResult.getMatchedCount()).isOne();
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, a: 2, b: 3"));
    }

    @Test
    public void testUpsertWithEmbeddedQuery() {
        collection.updateOne(json("_id: 1, 'e.i': 1"), json("$set: {a: 1}"), new UpdateOptions().upsert(true));
        assertThat(collection.find(json("_id: 1")).first()).isEqualTo(json("_id: 1, e: {i: 1}, a: 1"));
    }

    @Test
    public void testUpsertWithIdIn() throws Exception {
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
    public void testUpsertWithId() throws Exception {
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
    public void testUpsertWithId_duplicateKey() throws Exception {
        collection.insertOne(json("_id: 'someid', somekey: 'other value'"));

        Document query = json("somekey: 'some value'");
        Document update = json("$set: { _id: 'someid', somekey: 'some value' }");

        assertMongoWriteException(() -> collection.updateOne(query, update, new UpdateOptions().upsert(true)),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: _id_ dup key: { _id: \"someid\" }");
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/93
    @Test
    public void testReplaceOneWithId() throws Exception {
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

    @Test
    public void testReplaceOneUpsertsWithGeneratedId() throws Exception {
        collection.replaceOne(json("value: 'abc'"), json("value: 'abc'"), new ReplaceOptions().upsert(true));

        assertThat(collection.find())
            .extracting(document -> document.get("value"))
            .containsExactly("abc");

        assertThat(collection.find().first().get("_id"))
            .isInstanceOf(ObjectId.class);
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/41
    @Test
    public void testBulkUpsert() throws Exception {
        List<ReplaceOneModel<Document>> models = Arrays.asList(
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
            .containsExactlyInAnyOrder(json("_id: 1, a: 1"), json("_id: 2, a: 1"));

        models = Arrays.asList(
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
    public void testUpdateWithMultiplyOperator() throws Exception {
        Document object = json("_id: 1");

        collection.insertOne(object);

        collection.updateOne(object, json("$mul: {a: 2}, $set: {b: 2}"));
        assertThat(collection.find(object).first()).isEqualTo(json("_id: 1, a: 0, b: 2"));

        collection.updateOne(object, json("$mul: {b: 2.5}, $inc: {a: 0.5}"));
        assertThat(collection.find(object).first()).isEqualTo(json("_id: 1, a: 0.5, b: 5.0"));
    }

    @Test
    public void testUpdateWithIllegalMultiplyFails() throws Exception {
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
    public void testIsMaster() throws Exception {
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
    public void testFindWithNullOrNoFieldFilter() {

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
    public void testInsertWithIllegalId() throws Exception {
        assertMongoWriteException(() -> collection.insertOne(json("_id: [1, 2, 3]")),
            2, "BadValue", "can't use an array for _id");
    }

    @Test
    public void testInsertsWithUniqueIndex() {
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
    public void testInsertBinaryData() throws Exception {
        collection.insertOne(new Document("test", new byte[] { 0x01, 0x02, 0x03 }));
    }

    // see https://github.com/bwaldvogel/mongo-java-server/issues/9
    @Test
    public void testUniqueIndexWithSubdocument() {
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
    public void testUniqueIndexWithDeepDocuments() throws Exception {
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
    public void testSecondaryUniqueIndexUpdate() throws Exception {
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
    public void testSecondarySparseUniqueIndex() throws Exception {
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
    public void testCompoundSparseUniqueIndex() throws Exception {
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
    public void testCompoundSparseUniqueIndexOnEmbeddedDocuments() throws Exception {
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
    public void testUpdateWithSparseUniqueIndex() throws Exception {
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
    public void testSparseUniqueIndexOnEmbeddedDocument() throws Exception {
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

    @Test
    public void testAddNonUniqueIndexOnNonIdField() {
        collection.insertOne(json("someField: 'abc'"));
        assertThat(collection.listIndexes()).hasSize(1);

        collection.createIndex(new Document("someField", 1), new IndexOptions().unique(false));
        assertThat(collection.listIndexes()).hasSize(2);

        collection.insertOne(json("someField: 'abc'"));
    }

    @Test
    public void testAddSparseIndexOnNonIdField() {
        collection.insertOne(json("someField: 'abc'"));
        assertThat(collection.listIndexes()).hasSize(1);

        collection.createIndex(new Document("someField", 1), new IndexOptions().sparse(true));
        assertThat(collection.listIndexes()).hasSize(2);

        collection.insertOne(json("someField: 'abc'"));
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/61
    @Test
    public void testDeleteAllDocumentsWithUniqueSparseIndex() {
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
    public void testAddPartialIndexOnNonIdField() {
        collection.insertOne(json("someField: 'abc'"));
        assertThat(collection.listIndexes()).hasSize(1);

        collection.createIndex(new Document("someField", 1), new IndexOptions()
            .partialFilterExpression(json("someField: {$gt: 5}")));

        assertThat(collection.listIndexes()).hasSize(2);

        collection.insertOne(json("someField: 'abc'"));
    }

    @Test
    public void testCompoundUniqueIndices() {
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
    public void testCompoundUniqueIndices_Subdocument() {
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
    public void testCompoundUniqueIndicesWithInQuery() {
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
    public void testAddUniqueIndexOnExistingDocuments() throws Exception {
        collection.insertOne(json("_id: 1, value: 'a'"));
        collection.insertOne(json("_id: 2, value: 'b'"));
        collection.insertOne(json("_id: 3, value: 'c'"));

        collection.createIndex(json("value: 1"), new IndexOptions().unique(true));

        assertMongoWriteException(() -> collection.insertOne(json("value: 'c'")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: value_1 dup key: { value: \"c\" }");

        collection.insertOne(json("_id: 4, value: 'd'"));
    }

    @Test
    public void testAddUniqueIndexOnExistingDocuments_violatingUniqueness() throws Exception {
        collection.insertOne(json("_id: 1, value: 'a'"));
        collection.insertOne(json("_id: 2, value: 'b'"));
        collection.insertOne(json("_id: 3, value: 'c'"));
        collection.insertOne(json("_id: 4, value: 'b'"));

        assertThatExceptionOfType(DuplicateKeyException.class)
            .isThrownBy(() -> collection.createIndex(json("value: 1"), new IndexOptions().unique(true)))
            .withMessage("Write failed with error code 11000 and error message " +
                "'E11000 duplicate key error collection: testdb.testcoll index: value_1 dup key: { value: \"b\" }'");

        assertThat(collection.listIndexes())
            .containsExactly(json("name: '_id_', ns: 'testdb.testcoll', key: {_id: 1}, v: 2"));

        collection.insertOne(json("_id: 5, value: 'a'"));
    }

    @Test
    public void testCursorOptionNoTimeout() throws Exception {
        try (MongoCursor<Document> cursor = collection.find().noCursorTimeout(true).iterator()) {
            assertThat(cursor.hasNext()).isFalse();
        }
    }

    @Test
    public void testBulkInsert() throws Exception {
        List<WriteModel<Document>> inserts = new ArrayList<>();
        inserts.add(new InsertOneModel<>(json("_id: 1")));
        inserts.add(new InsertOneModel<>(json("_id: 2")));
        inserts.add(new InsertOneModel<>(json("_id: 3")));

        BulkWriteResult result = collection.bulkWrite(inserts);
        assertThat(result.getInsertedCount()).isEqualTo(3);
    }

    @Test
    public void testBulkUpdateOrdered() throws Exception {
        testBulkUpdate(true);
    }

    @Test
    public void testBulkUpdateUnordered() throws Exception {
        testBulkUpdate(false);
    }

    private void testBulkUpdate(boolean ordered) {
        insertUpdateInBulk(ordered);
        removeInBulk(ordered);
        insertUpdateInBulkNoMatch(ordered);
    }

    @Test
    public void testUpdateCurrentDateIllegalTypeSpecification() throws Exception {
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
    public void testUpdateCurrentDate() throws Exception {
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
    public void testRenameField() throws Exception {
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
    public void testRenameField_embeddedDocument() {
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
            .isThrownBy(() -> collection.updateOne(json("_id: 1"), json("$rename: {'foo.b.c': 'foo.b.d'}")
            ));
    }

    @Test
    public void testRenameFieldIllegalValue() throws Exception {
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
    public void testRenameCollection() throws Exception {
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
                json("name: '_id_', ns: 'testdb.other-collection-name', key: {_id: 1}, v: 2"),
                json("name: 'a_1', ns: 'testdb.other-collection-name', key: {a: 1}, unique: true, v: 2")
            );

        assertThat(collection.listIndexes()).isEmpty();
    }

    @Test
    public void testRenameCollection_targetAlreadyExists() throws Exception {
        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));
        collection.insertOne(json("_id: 3"));

        MongoCollection<Document> otherCollection = db.getCollection("other-collection-name");
        otherCollection.insertOne(json("_id: 1"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.renameCollection(new MongoNamespace(db.getName(), "other-collection-name")))
            .withMessageContaining("Command failed with error 48 (NamespaceExists): 'target namespace exists'");

        assertThat(db.listCollectionNames())
            .containsExactlyInAnyOrder(getCollectionName(), "other-collection-name");

        assertThat(collection.countDocuments()).isEqualTo(3);
        assertThat(getCollection("other-collection-name").countDocuments()).isEqualTo(1);
    }

    @Test
    public void testRenameCollection_dropTarget() throws Exception {
        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));
        collection.insertOne(json("_id: 3"));

        MongoCollection<Document> otherCollection = db.getCollection("other-collection-name");
        otherCollection.insertOne(json("_id: 1"));

        collection.renameCollection(new MongoNamespace(db.getName(), "other-collection-name"),
            new RenameCollectionOptions().dropTarget(true));

        assertThat(db.listCollectionNames()).containsExactly("other-collection-name");

        assertThat(getCollection("other-collection-name").countDocuments()).isEqualTo(3);
    }

    @Test
    public void testListIndexes_empty() throws Exception {
        assertThat(collection.listIndexes()).isEmpty();
    }

    @Test
    public void testListIndexes() throws Exception {
        collection.insertOne(json("_id: 1"));
        MongoCollection<Document> other = db.getCollection("other");
        other.insertOne(json("_id: 1"));

        collection.createIndex(json("bla: 1"));

        collection.createIndex(new Document("a", 1), new IndexOptions().unique(true));
        collection.createIndex(new Document("a", 1).append("b", -1.0), new IndexOptions().unique(true));

        assertThat(collection.listIndexes())
            .containsExactlyInAnyOrder(
                json("name: '_id_', ns: 'testdb.testcoll', key: {_id: 1}, v: 2"),
                json("name: 'bla_1', ns: 'testdb.testcoll', key: {bla: 1}, v: 2"),
                json("name: 'a_1', ns: 'testdb.testcoll', key: {a: 1}, unique: true, v: 2"),
                json("name: 'a_1_b_-1', ns: 'testdb.testcoll', key: {a: 1, b: -1.0}, unique: true, v: 2")
            );

        assertThat(other.listIndexes())
            .containsExactlyInAnyOrder(
                json("name: '_id_', ns: 'testdb.other', key: {_id: 1}, v: 2")
            );
    }

    @Test
    public void testFieldSelection_deselectId() {
        collection.insertOne(json("_id: 1, order:1, visits: 2"));

        Document document = collection.find(json("")).projection(json("_id: 0")).first();
        assertThat(document).isEqualTo(json("order:1, visits:2"));
    }

    @Test
    public void testFieldSelection_deselectOneField() {
        Document obj = json("_id: 1, order:1, visits: 2, eid: 12345");
        collection.insertOne(obj);

        Document document = collection.find(new Document()).projection(json("visits: 0")).first();
        assertThat(document).isEqualTo(json("_id: 1, order:1, eid: 12345"));
    }

    @Test
    public void testFieldSelection_deselectTwoFields() {
        Document obj = json("_id: 1, order:1, visits: 2, eid: 12345");
        collection.insertOne(obj);

        Document document = collection.find(new Document()).projection(json("visits: 0, eid: 0")).first();
        assertThat(document).isEqualTo(json("_id: 1, order:1"));
    }

    @Test
    public void testFieldSelection_selectAndDeselectFields() {
        Document obj = json("_id: 1, order:1, visits: 2, eid: 12345");
        collection.insertOne(obj);

        Document document = collection.find(new Document()).projection(json("visits: 0, eid: 1")).first();
        assertThat(document).isEqualTo(json("_id: 1, eid: 12345"));
    }

    @Test
    public void testPullWithInPattern() {

        collection.insertOne(json("_id: 1, tags: ['aa', 'bb', 'ab', 'cc']"));

        collection.updateOne(json("_id: 1"), pullByFilter(in("tags", Pattern.compile("a+"))));

        assertThat(collection.find().first()).isEqualTo(json("_id: 1, tags: ['bb', 'cc']"));
    }

    @Test
    public void testPullWithInPatternAnchored() {

        collection.insertOne(json("_id: 1, tags: ['aa', 'bb', 'ab', 'cc']"));

        collection.updateOne(json("_id: 1"), pullByFilter(in("tags", Pattern.compile("^a+$"))));

        assertThat(collection.find().first()).isEqualTo(json("_id: 1, tags: ['bb', 'ab', 'cc']"));
    }

    @Test
    public void testPullWithInNumbers() {

        collection.insertOne(json("_id: 1, values: [1, 2, 2.5, 3.0, 4]"));

        collection.updateOne(json("_id: 1"), pullByFilter(in("values", Arrays.asList(2.0, 3, 4L))));

        assertThat(collection.find().first()).isEqualTo(json("_id: 1, values: [1, 2.5]"));
    }

    @Test
    public void testDocumentWithHashMap() {
        Map<String, String> value = new HashMap<>();
        value.put("foo", "bar");

        collection.insertOne(new Document("_id", 1).append("map", value));
        Bson document = collection.find().first();
        assertThat(document).isEqualTo(json("_id: 1, map: {foo: 'bar'}"));
    }

    @Test
    public void testFindAndOfOrs() throws Exception {
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
    public void testInOperatorWithNullValue() {
        collection.insertMany(Arrays.asList(
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
    public void testQueryWithReference() throws Exception {
        collection.insertOne(json("_id: 1"));
        String collectionName = getCollectionName();
        collection.insertOne(new Document("_id", 2).append("ref", new DBRef(collectionName, 1)));
        collection.insertOne(new Document("_id", 3).append("ref", new DBRef(collectionName, 2)));

        Document doc = collection.find(new Document("ref", new DBRef(collectionName, 1))).projection(json("_id: 1")).first();
        assertThat(doc).isEqualTo(json("_id: 2"));
    }

    @Test
    public void testQueryWithIllegalReference() throws Exception {
        collection.insertOne(json("_id: 1"));
        String collectionName = getCollectionName();
        collection.insertOne(new Document("_id", 2).append("ref", new DBRef(collectionName, 1)));
        collection.insertOne(new Document("_id", 3).append("ref", new DBRef(collectionName, 2)));

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(json("ref: {$ref: 'coll'}")).first())
            .withMessageContaining("Query failed with error code 2 and error message 'unknown operator: $ref'");
    }

    @Test
    public void testAndOrNorWithEmptyArray() throws Exception {
        collection.insertOne(json(""));

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(and()).first())
            .withMessageContaining("Query failed with error code 2 and error message '$and/$or/$nor must be a nonempty array'");

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(nor()).first())
            .withMessageContaining("Query failed with error code 2 and error message '$and/$or/$nor must be a nonempty array'");

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(or()).first())
            .withMessageContaining("Query failed with error code 2 and error message '$and/$or/$nor must be a nonempty array'");
    }

    @Test
    public void testInsertLargeDocument() throws Exception {
        insertAndFindLargeDocument(100, 1);
        insertAndFindLargeDocument(1000, 2);
        insertAndFindLargeDocument(10000, 3);
    }

    @Test
    public void testInsertAndUpdateAsynchronously() throws Exception {
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
            asyncCollection.insertOne(document).subscribe(new Subscriber<Success>() {
                @Override
                public void onSubscribe(Subscription s) {
                    s.request(Integer.MAX_VALUE);
                }

                @Override
                public void onNext(Success success) {
                    log.info("inserted {}", document);
                    Document query = new Document("_id", document.getInteger("_id"));
                    asyncCollection.updateOne(query, Updates.set("updated", true)).subscribe(new Subscriber<UpdateResult>() {
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
    public void testAllQuery() throws Exception {
        // see https://docs.mongodb.com/manual/reference/operator/query/all/
        collection.insertOne(new Document("_id", new ObjectId("5234cc89687ea597eabee675"))
            .append("code", "xyz")
            .append("tags", Arrays.asList("school", "book", "bag", "headphone", "appliance"))
            .append("qty", Arrays.asList(
                new Document().append("size", "S").append("num", 10).append("color", "blue"),
                new Document().append("size", "M").append("num", 45).append("color", "blue"),
                new Document().append("size", "L").append("num", 100).append("color", "green")
            )));

        collection.insertOne(new Document("_id", new ObjectId("5234cc8a687ea597eabee676"))
            .append("code", "abc")
            .append("tags", Arrays.asList("appliance", "school", "book"))
            .append("qty", Arrays.asList(
                new Document().append("size", "6").append("num", 100).append("color", "green"),
                new Document().append("size", "6").append("num", 50).append("color", "blue"),
                new Document().append("size", "8").append("num", 100).append("color", "brown")
            )));

        collection.insertOne(new Document("_id", new ObjectId("5234ccb7687ea597eabee677"))
            .append("code", "efg")
            .append("tags", Arrays.asList("school", "book"))
            .append("qty", Arrays.asList(
                new Document().append("size", "S").append("num", 10).append("color", "blue"),
                new Document().append("size", "M").append("num", 100).append("color", "blue"),
                new Document().append("size", "L").append("num", 100).append("color", "green")
            )));

        collection.insertOne(new Document("_id", new ObjectId("52350353b2eff1353b349de9"))
            .append("code", "ijk")
            .append("tags", Arrays.asList("electronics", "school"))
            .append("qty", Collections.singletonList(
                new Document().append("size", "M").append("num", 100).append("color", "green")
            )));

        assertThat(collection.find(json("tags: {$all: ['appliance', 'school', 'book']}")))
            .extracting(d -> d.get("_id"))
            .containsExactly(new ObjectId("5234cc89687ea597eabee675"), new ObjectId("5234cc8a687ea597eabee676"));
    }

    @Test
    public void testMatchesElementQuery() throws Exception {
        collection.insertOne(json("_id: 1, results: [82, 85, 88]"));
        collection.insertOne(json("_id: 2, results: [75, 88, 89]"));

        assertThat(collection.find(json("results: {$elemMatch: {$gte: 80, $lt: 85}}")))
            .containsExactly(json("_id: 1, results: [82, 85, 88]"));
    }

    @Test
    public void testMatchesElementInEmbeddedDocuments() throws Exception {
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
    public void testElemMatchWithExpression() throws Exception {
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
    public void testElemMatchAndAllQuery() throws Exception {
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
    public void testQueryWithElemMatch() {
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
    public void testProjectionWithElemMatch() {
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
    public void testProjectionWithElemMatch_BigSubdocument() {
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
    public void testQueryWithProjection_elemMatchAndPositionalOperator() throws Exception {
        collection.insertOne(json("_id: 1, states: [{state: 'A', key: 'abc'}, {state: 'B', key: 'efg'}]"));
        collection.insertOne(json("_id: 2, states: [{state: 'B', key: 'abc'}, {state: 'B', key: 'efg'}]"));

        assertThat(collection.find(json("states: {$elemMatch: {state: {$eq: 'A'}, key: {$eq: 'abc'}}}")).
            projection(json("'states.$': 1")))
            .containsExactly(json("_id: 1, states: [{state: 'A', key: 'abc'}]"));
    }

    @Test
    public void testProjectionWithSlice() throws Exception {
        collection.insertOne(json("_id: 1, values: ['a', 'b', 'c', 'd', 'e']"));
        collection.insertOne(json("_id: 2, values: 'xyz'"));

        assertThat(collection.find(json("_id: 1")).projection(json("values: {$slice: 1}")))
            .containsExactly(json("_id: 1, values: ['a']"));

        assertThat(collection.find(json("_id: 1")).projection(json("values: {$slice: ['xyz', 2]}")))
            .containsExactly(json("_id: 1, values: ['a', 'b']"));

        assertThat(collection.find(json("_id: 1")).projection(json("values: {$slice: [-3, 2]}")))
            .containsExactly(json("_id: 1, values: ['c', 'd']"));

        assertThat(collection.find(json("_id: 2")).projection(json("values: {$slice: 1}")))
            .containsExactly(json("_id: 2, values: 'xyz'"));

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(json("_id: 1")).projection(json("values: {$slice: ['$_id', '$_id']}")).first())
            .withMessageContaining("Query failed with error code 2 and error message '$slice limit must be positive'");

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(json("_id: 1")).projection(json("values: {$slice: [1, 0]}")).first())
            .withMessageContaining("Query failed with error code 2 and error message '$slice limit must be positive'");

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(json("_id: 1")).projection(json("values: {$slice: [1, 'xyz']}")).first())
            .withMessageContaining("Query failed with error code 2 and error message '$slice limit must be positive'");

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(json("_id: 1")).projection(json("values: {$slice: [1, 2, 3]}")).first())
            .withMessageContaining("Query failed with error code 2 and error message '$slice array wrong size'");

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(json("_id: 1")).projection(json("values: {$slice: 'abc'}")).first())
            .withMessageContaining("Query failed with error code 2 and error message '$slice only supports numbers and [skip, limit] arrays'");
    }

    @Test
    public void testMatchesNullOrMissing() throws Exception {
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
    public void testIllegalElementMatchQuery() throws Exception {
        collection.insertOne(json("_id: 1, results: [ 82, 85, 88 ]"));

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(json("results: {$elemMatch: [ 85 ]}")).first())
            .withMessageContaining("Query failed with error code 2 and error message '$elemMatch needs an Object'");

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(json("results: {$elemMatch: 1}")).first())
            .withMessageContaining("Query failed with error code 2 and error message '$elemMatch needs an Object'");
    }

    @Test
    public void testQueryWithOperatorAndWithoutOperator() throws Exception {
        collection.insertOne(json("_id: 1, x: {y: 23}"));
        collection.insertOne(json("_id: 2, x: 9"));
        collection.insertOne(json("_id: 3, x: 100"));

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(json("x: {$lt: 10, y: 23}")).first())
            .withMessageContaining("Query failed with error code 2 and error message 'unknown operator: y'");

        assertThat(collection.find(json("x: {y: 23, $lt: 10}"))).isEmpty();
        assertThat(collection.find(json("x: {y: {$lt: 100, z: 23}}"))).isEmpty();
        assertThat(collection.find(json("a: 123, x: {y: {$lt: 100, z: 23}}"))).isEmpty();
    }

    @Test
    public void testQueryWithComment() throws Exception {
        collection.insertOne(json("_id: 1, x: 2"));
        collection.insertOne(json("_id: 2, x: 3"));
        collection.insertOne(json("_id: 3, x: 4"));

        assertThat(collection.find(json("x: {$mod: [2, 0 ]}, $comment: 'Find even values.'")))
            .extracting(d -> d.get("_id"))
            .containsExactly(1, 3);
    }

    @Test
    public void testValidate() throws Exception {
        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> db.runCommand(new Document("validate", getCollectionName())))
            .withMessageContaining("Command failed with error 26 (NamespaceNotFound): 'ns not found'");

        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));
        collection.insertOne(json("_id: 3"));

        collection.deleteOne(json("_id: 2"));

        Document result = db.runCommand(new Document("validate", getCollectionName()));
        assertThat(result.get("nrecords")).isEqualTo(2);
    }

    @Test
    public void testGetLastError() throws Exception {
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
    public void testResetError() throws Exception {
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
    public void testIllegalTopLevelOperator() throws Exception {
        Document query = json("$illegalOperator: 1");

        collection.insertOne(json("_id: 1"));

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(query).first())
            .withMessageContaining("Query failed with error code 2 and error message 'unknown top level operator: $illegalOperator'");
    }

    @Test
    public void testExprQuery() throws Exception {
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
    public void testExprQuery_IllegalFieldPath() throws Exception {
        collection.insertOne(json("_id: 1"));

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(json("$expr: {$eq: ['$a.', 10]}")).first())
            .withMessageContaining("Query failed with error code 40353 and error message 'FieldPath must not end with a '.'.'");

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(json("$expr: {$eq: ['$.a', 10]}")).first())
            .withMessageContaining("Query failed with error code 15998 and error message 'FieldPath field names may not be empty strings.'");

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(json("$expr: {$eq: ['$a..1', 10]}")).first())
            .withMessageContaining("Query failed with error code 15998 and error message 'FieldPath field names may not be empty strings.'");
    }

    @Test
    public void testQueryEmbeddedDocument() throws Exception {
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
            .containsExactlyInAnyOrder(json("_id: 8, b: {c: {d: 1, e: 2}}"));

        assertThat(collection.find(json("'b.c': {d: 1, e: 2}")))
            .containsExactlyInAnyOrder(json("_id: 8, b: {c: {d: 1, e: 2}}"));

        assertThat(collection.find(json("'b.c.e': 2")))
            .containsExactlyInAnyOrder(json("_id: 8, b: {c: {d: 1, e: 2}}"));
    }

    @Test
    public void testQueryWithEquivalentEmbeddedDocument() throws Exception {
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
    public void testOrderByMissingAndNull() throws Exception {
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
    public void testOrderByEmbeddedDocument() throws Exception {
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
    public void testFindByListValue() throws Exception {
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
    public void testFindAndOrderByWithListValues() throws Exception {
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
    public void testDistinctEmbeddedDocument() throws Exception {
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
    public void testEmptyArrayQuery() throws Exception {
        collection.insertOne(json("_id: 1"));

        assertThatExceptionOfType(MongoQueryException.class)
            .isThrownBy(() -> collection.find(Filters.and()).first())
            .withMessageContaining("must be a nonempty array");
    }

    @Test
    public void testFindAllReferences() throws Exception {
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
    public void testInsertAndQueryNegativeZero() throws Exception {
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
    public void testUniqueIndexWithNegativeZero() throws Exception {
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
    public void testDecimal128() throws Exception {
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
    public void testDecimal128_Inc() throws Exception {
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
    public void testArrayNe() throws Exception {
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
    public void testExistsQuery() throws Exception {
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
    public void testExistsQueryWithArray() throws Exception {
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
    public void testExistsQueryWithTrailingDot() throws Exception {
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
    public void testRegExQuery() throws Exception {
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
    public void testInsertAndFindJavaScriptContent() throws Exception {
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
        BulkWriteResult result = collection.bulkWrite(Collections.singletonList(deleteOp),
            new BulkWriteOptions().ordered(ordered));

        assertThat(result.getDeletedCount()).isEqualTo(3);
        assertThat(collection.countDocuments()).isZero();
    }

    @FunctionalInterface
    protected interface Callable {
        void call();
    }

    protected static void assertMongoWriteException(Callable callable, int expectedErrorCode, String expectedMessage) {
        assertMongoWriteException(callable, expectedErrorCode, "Location" + expectedErrorCode, expectedMessage);
    }

    protected static void assertMongoWriteException(Callable callable, int expectedErrorCode, String expectedCodeName,
                                                    String expectedMessage) {
        try {
            callable.call();
            fail("MongoWriteException expected");
        } catch (MongoWriteException e) {
            assertThat(e).hasMessage(expectedMessage);
            assertThat(e.getError().getCode()).isEqualTo(expectedErrorCode);

            Document actual = db.runCommand(json("getlasterror: 1"));
            assertThat(actual.getString("codeName")).isEqualTo(expectedCodeName);
        }
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/76
    @Test
    public void testInsertWithoutId() throws Exception {
        DocumentCodec documentCodec = Mockito.spy(new DocumentCodec());
        Mockito.doAnswer(AdditionalAnswers.returnsFirstArg()).when(documentCodec).generateIdIfAbsentFromDocument(Mockito.any());

        MongoClientOptions mongoClientOptions = MongoClientOptions.builder()
            .codecRegistry(CodecRegistries.fromCodecs(documentCodec))
            .build();

        try (MongoClient mongoClient = new MongoClient(new ServerAddress(serverAddress), mongoClientOptions)) {
            MongoDatabase database = mongoClient.getDatabase(db.getName());
            MongoCollection<Document> collection = database.getCollection(getCollectionName());
            collection.insertOne(json("x: 1"));

            assertThat(collection.find(json("x: 1")).first().get("_id"))
                .isInstanceOf(ObjectId.class);
        }

        Mockito.verify(documentCodec).generateIdIfAbsentFromDocument(Mockito.any());
    }

    @Test
    public void testMultikeyIndex_simpleArrayValues() throws Exception {
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
    public void testCompoundMultikeyIndex_simpleArrayValues() throws Exception {
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
    public void testCompoundMultikeyIndex_threeKeys() throws Exception {
        collection.createIndex(json("b: 1, a: 1, c: 1"), new IndexOptions().unique(true));

        assertMongoWriteException(() -> collection.insertOne(json("b: [1, 2, 3], a: ['abc'], c: ['x', 'y']")),
            171, "CannotIndexParallelArrays", "cannot index parallel arrays [a] [b]");
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/69
    @Test
    public void testCompoundMultikeyIndex_documents() throws Exception {
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
    public void testCompoundMultikeyIndex_deepDocuments() throws Exception {
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
    public void testUpdateArrayWithPositionalAll() {
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
    public void testUpdateArrayWithPositionalAll_NullValue() {
        collection.insertOne(json("_id: 1, grades: [1, 2, null, 3]"));

        collection.findOneAndUpdate(
            json("_id: 1"),
            json("$set: {'grades.$[]': 'abc'}"));

        assertThat(collection.find(json("_id: 1")).first())
            .isEqualTo(json("_id: 1, grades: ['abc', 'abc', 'abc', 'abc']"));
    }

    @Test
    public void testUpdateArrayWithPositionalAllAndArrayFilter() {
        collection.insertOne(json("_id: 1, grades: [{x: [1, 2, 3]}, {x: [3, 4, 5]}, {x: [1, 2, 3]}]"));

        collection.findOneAndUpdate(
            json("_id: 1"),
            json("$inc: {'grades.$[].x.$[element]': 1}"),
            new FindOneAndUpdateOptions().arrayFilters(Arrays.asList(json("element: {$gte: 3}"))));

        assertThat(collection.find(json("_id: 1")).first())
            .isEqualTo(json("_id: 1, grades: [{x: [1, 2, 4]}, {x: [4, 5, 6]}, {x: [1, 2, 4]}]"));
    }

    @Test
    public void testUpdateArrayOfDocumentsWithPositionalAll() {
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
    public void testIllegalUpdateWithPositionalAll() {
        collection.insertOne(json("_id: 1, a: {b: [1, 2, 3]}"));
        collection.insertOne(json("_id: 2, a: {b: 5}"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(json("_id: 1"), json("$set: {'a.$[]': 'abc'}")))
            .withMessageContaining("Command failed with error 2 (BadValue): 'Cannot apply array updates to non-array element a: { b: [ 1, 2, 3 ] }");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(json("_id: 2"), json("$set: {'a.b.$[]': 'abc'}")))
            .withMessageContaining("Command failed with error 2 (BadValue): 'Cannot apply array updates to non-array element b: 5");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(json("_id: 1"), json("$set: {'a.b.$[].c': 'abc'}")))
            .withMessageContaining("Command failed with error 28 (PathNotViable): 'Cannot create field 'c' in element {0: 1}");
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/82
    @Test
    public void testUpsertWithPositionalAll() throws Exception {
        Document result = collection.findOneAndUpdate(json("_id: 1, a: [5, 8]"), json("$set: {'a.$[]': 1}"),
            new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));

        assertThat(result).isEqualTo(json("_id: 1, a: [1, 1]"));
    }

    @Test
    public void testUpdateWithMultipleArrayFiltersInOnePath() throws Exception {
        collection.insertOne(json("_id: 1, grades: [{value: 10, x: [1, 2]}, {value: 20, x: [3, 4]}]"));

        collection.findOneAndUpdate(
            json("_id: 1"),
            json("$set: {'grades.$[element].x.$[]': 'abc'}"),
            new FindOneAndUpdateOptions().arrayFilters(Arrays.asList(json("'element.value': {$gt: 10}"))));

        assertThat(collection.find(json("_id: 1")).first())
            .isEqualTo(json("_id: 1, grades: [{value: 10, x: [1, 2]}, {value: 20, x: ['abc', 'abc']}]"));

        collection.findOneAndUpdate(
            json("_id: 1"),
            json("$set: {'grades.0.x.$[element]': 'abc'}"),
            new FindOneAndUpdateOptions().arrayFilters(Arrays.asList(json("'element': {$gt: 1}"))));

        assertThat(collection.find(json("_id: 1")).first())
            .isEqualTo(json("_id: 1, grades: [{value: 10, x: [1, 'abc']}, {value: 20, x: ['abc', 'abc']}]"));
    }

    @Test
    public void testUpdateArrayWithMultiplePositionalAll() {
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
            .withMessageContaining("Command failed with error 2 (BadValue): 'The path 'grades.2.c' must exist in the document in order to apply array updates.");
    }

    @Test
    public void testUpdateArrayWithMultiplePositionalAll_Simple() {
        collection.insertOne(json("_id: 1, grades: [[1, 2], [3, 4]]"));

        collection.findOneAndUpdate(
            json("_id: 1"),
            json("$set: {'grades.$[].$[]': 1}"));

        assertThat(collection.find(json("_id: 1")).first())
            .isEqualTo(json("_id: 1, grades: [[1, 1], [1, 1]]"));
    }

    @Test
    public void testUpdateArrayWithIllegalMultiplePositionalAll() {
        collection.insertOne(json("_id: 1, grades: [[[1, 2], [3, 4]], [[4, 5], [2, 3]]]"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.findOneAndUpdate(
                json("_id: 1"),
                json("$inc: {'grades.$[].$[]': 1}")))
            .withMessageContaining("Command failed with error 14 (TypeMismatch): 'Cannot apply $inc to a value of non-numeric type. {_id: 1} has the field '0' of non-numeric type array");
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/98
    @Test
    public void testGetKeyValues_multiKey_document_nested_objects() throws Exception {
        collection.createIndex(json("'stock.size': 1, 'stock.quantity': 1"), new IndexOptions().unique(true));

        collection.insertOne(json("stock: [{size: 'S', quantity: 10}]"));
        collection.insertOne(json("stock: [{size: 'M', quantity: 10}, {size: 'L', quantity: 10}]"));
        collection.insertOne(json("stock: [{size: 'S', quantity: 20}]"));
        collection.insertOne(json("stock: [{quantity: 20}]"));
        collection.insertOne(json("stock: [{size: 'M'}]"));
        collection.insertOne(json("stock: {size: ['XL', 'XXL']}"));

        assertThatExceptionOfType(MongoWriteException.class)
            .isThrownBy(() -> collection.insertOne(json("stock: {size: ['S', 'M'], quantity: [30, 40]}")))
            .withMessage("cannot index parallel arrays [quantity] [size]");

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
    public void testComparisons() throws Exception {
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
    public void testMinKeyComparison() {
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
    }

    @Test
    public void testMaxKeyComparison() {
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
    }

    @Test
    public void testOldAndNewUuidTypes() throws Exception {
        Document document1 = new Document("_id", UUID.fromString("5542cbb9-7833-96a2-b456-f13b6ae1bc80"));
        collection.insertOne(document1);

        assertMongoWriteException(() -> collection.insertOne(document1),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: _id_ dup key: { _id: BinData(3, A2963378B9CB425580BCE16A3BF156B4) }");
        try (MongoClient standardUuidClient = getClientWithStandardUuid()) {
            MongoCollection<Document> collectionStandardUuid = standardUuidClient.getDatabase(AbstractTest.collection.getNamespace().getDatabaseName()).getCollection(AbstractTest.collection.getNamespace().getCollectionName());

            collectionStandardUuid.insertOne(document1);

            assertMongoWriteException(() -> collectionStandardUuid.insertOne(document1),
                11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: _id_ dup key: { _id: UUID(\"5542cbb9-7833-96a2-b456-f13b6ae1bc80\") }");

            Document document2 = new Document("_id", UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"));
            collectionStandardUuid.insertOne(document2);

            collection.deleteOne(document1);

            assertThat(collectionStandardUuid.find().sort(json("_id: 1")))
                .containsExactly(
                    document1,
                    document2
                );
        }
    }

    @Test
    public void testNewUuidType() throws Exception {
        try (MongoClient standardUuidClient = getClientWithStandardUuid()) {
            MongoCollection<Document> collectionStandardUuid = standardUuidClient.getDatabase(AbstractTest.collection.getNamespace().getDatabaseName()).getCollection(AbstractTest.collection.getNamespace().getCollectionName());

            collectionStandardUuid.insertOne(new Document("_id", UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-000000000001")));
            collectionStandardUuid.insertOne(new Document("_id", UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-000000000002")));
            collectionStandardUuid.insertOne(new Document("_id", UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-000000000003")));
        }
    }

    @Test
    void testConnectionStatus() throws Exception {
        Document result = runCommand("connectionStatus");
        assertThat(result).isEqualTo(json("ok: 1.0, authInfo: {authenticatedUsers: [], authenticatedUserRoles: []}"));
    }

    @Test
    void testHostInfo() throws Exception {
        Document result = runCommand("hostInfo");
        assertThat(result.get("ok")).isEqualTo(1.0);
        assertThat(result).containsKeys("os", "system", "extra");
    }

    @Test
    void testGetCmdLineOpts() throws Exception {
        Document result = runCommand("getCmdLineOpts");
        assertThat(result.get("ok")).isEqualTo(1.0);
        assertThat(result).containsOnlyKeys("ok", "argv", "parsed");
    }

    @Test
    public void testUpdateWithExpressionIsNotPossible() throws Exception {
        collection.insertOne(json("_id: 1"));

        assertMongoWriteException(
            () -> collection.updateOne(json("_id: 1"), json("$set: {x: {$expr: {$add: ['$_id', 10]}}}")),
            52, "DollarPrefixedFieldName", "The dollar ($) prefixed field '$expr' in 'x.$expr' is not valid for storage.");
    }

}
