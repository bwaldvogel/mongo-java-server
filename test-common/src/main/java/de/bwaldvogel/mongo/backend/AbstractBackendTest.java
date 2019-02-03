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
import static de.bwaldvogel.mongo.backend.TestUtils.getCollectionStatistics;
import static de.bwaldvogel.mongo.backend.TestUtils.json;
import static de.bwaldvogel.mongo.backend.TestUtils.toArray;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.fail;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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
import org.bson.BsonObjectId;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DBRef;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoQueryException;
import com.mongodb.MongoServerException;
import com.mongodb.MongoWriteException;
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

    private Document runCommand(String commandName) {
        return runCommand(new Document(commandName, Integer.valueOf(1)));
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

    @Test
    public void testSimpleInsert() throws Exception {
        collection.insertOne(json("_id: 1"));
    }

    @Test
    public void testSimpleInsertDelete() throws Exception {
        collection.insertOne(json("_id: 1"));
        collection.deleteOne(json("_id: 1"));
    }

    @Test
    public void testCreateCollection() throws Exception {
        String newCollectionName = "some-collection";
        assertThat(toArray(db.listCollectionNames())).doesNotContain(newCollectionName);
        db.createCollection(newCollectionName, new CreateCollectionOptions());
        assertThat(toArray(db.listCollectionNames())).contains(newCollectionName);
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
            9, "Unknown modifier: $foo");
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
    public void testCollectionStats() throws Exception {
        db.createCollection("other-collection");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(this::getCollStats)
            .withMessageContaining("Command failed with error -1: 'Collection [testdb.testcoll] not found.'");

        collection.insertOne(json(""));
        collection.insertOne(json("abc: 'foo'"));

        Document stats = getCollStats();
        assertThat(stats.getDouble("ok")).isEqualTo(1.0);
        assertThat(stats.getInteger("count")).isEqualTo(2);
        assertThat(stats.getInteger("size")).isEqualTo(57);
        assertThat(stats.getInteger("avgObjSize")).isEqualTo(28);
    }

    private Document getCollStats() {
        String collectionName = collection.getNamespace().getCollectionName();
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
        List<Document> results = toArray(collection.find(query));
        assertThat(results).containsExactly(
            json("_id: {n: 'a', t: 1}, foo: 'bar'"),
            json("_id: {n: 'a', t: 2}, foo: 'bar'"),
            json("_id: {n: 'a', t: 3}, foo: 'bar'"));
    }

    @Test
    public void testCompoundSort() {
        collection.insertOne(json("a:1, _id: 1"));
        collection.insertOne(json("a:2, _id: 5"));
        collection.insertOne(json("a:1, _id: 2"));
        collection.insertOne(json("a:2, _id: 4"));
        collection.insertOne(json("a:1, _id: 3"));

        List<Document> documents = toArray(collection.find().sort(json("a:1, _id: -1")));
        assertThat(documents).containsExactly(json("a: 1, _id: 3"), json("a: 1, _id: 2"), json("a: 1, _id: 1"),
            json("a: 2, _id: 5"), json("a: 2, _id: 4"));
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testCountCommand() {
        assertThat(collection.count()).isZero();
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
        List<Document> indexes = toArray(getCollection("system.indexes").find());
        assertThat(indexes).containsExactlyInAnyOrder(
            json("key: {_id: 1}").append("ns", collection.getNamespace().getFullName()).append("name", "_id_"),
            json("key: {n: 1}").append("ns", collection.getNamespace().getFullName()).append("name", "n_1"),
            json("key: {b: 1}").append("ns", collection.getNamespace().getFullName()).append("name", "b_1"));
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
            assertThat(collection.keySet()).containsExactlyInAnyOrder("name", "options", "type", "idIndex", "info");
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

        List<String> collectionNames = toArray(db.listCollectionNames());
        assertThat(collectionNames).containsExactlyInAnyOrder("foo", "bar");
    }

    @Test
    public void testSystemIndexes() throws Exception {
        getCollection("foo").insertOne(json(""));
        getCollection("bar").insertOne(json(""));

        MongoCollection<Document> systemIndexes = db.getCollection("system.indexes");
        assertThat(toArray(systemIndexes.find())).containsExactlyInAnyOrder(
            json("name: '_id_', ns: 'testdb.foo', key: {_id: 1}"),
            json("name: '_id_', ns: 'testdb.bar', key: {_id: 1}")
        );
    }

    @Test
    public void testSystemNamespaces() throws Exception {
        getCollection("foo").insertOne(json(""));
        getCollection("bar").insertOne(json(""));

        MongoCollection<Document> namespaces = db.getCollection("system.namespaces");
        assertThat(toArray(namespaces.find())).containsExactlyInAnyOrder(
            json("name: 'testdb.system.indexes'"),
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

        assertThat(toArray(collection.distinct("n", Integer.class)))
            .containsExactly(null, 3, 1, 2, 0);

        assertThat(toArray(collection.distinct("n", json("n: {$gt: 1}"), Integer.class)))
            .containsExactly(3, 2);

        assertThat(collection.distinct("foobar", String.class)).isEmpty();

        assertThat(collection.distinct("_id", Integer.class))
            .hasSize((int) collection.countDocuments());
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/44
    @Test
    public void testDistinctUuids() throws Exception {
        collection.insertOne(json("_id: 1, n: null"));
        collection.insertOne(json("_id: 2").append("n", new UUID(0, 1)));
        collection.insertOne(json("_id: 3").append("n", new UUID(1, 0)));
        collection.insertOne(json("_id: 4").append("n", new UUID(0, 2)));
        collection.insertOne(json("_id: 5").append("n", new UUID(1, 1)));
        collection.insertOne(json("_id: 6").append("n", new UUID(1, 0)));

        assertThat(toArray(collection.distinct("n", UUID.class)))
            .containsExactly(
                null,
                new UUID(0, 1),
                new UUID(1, 0),
                new UUID(0, 2),
                new UUID(1, 1)
            );
    }

    @Test
    public void testInsertQueryAndSortBinaryTypes() throws Exception {
        byte[] highBytes = new byte[16];
        for (int i = 0; i < highBytes.length; i++) {
            highBytes[i] = (byte) 0xFF;
        }

        collection.insertOne(json("_id: 1, n: null"));
        collection.insertOne(json("_id: 2").append("n", new UUID(0, 1)));
        collection.insertOne(json("_id: 3").append("n", new UUID(1, 0)));
        collection.insertOne(json("_id: 4, n: 'abc'"));
        collection.insertOne(json("_id: 5, n: 17"));
        collection.insertOne(json("_id: 6, n: [1, 2, 3]"));
        collection.insertOne(json("_id: 7").append("n", new byte[] { 0, 0, 0, 1 }));
        collection.insertOne(json("_id: 8").append("n", highBytes));
        collection.insertOne(json("_id: 9").append("n", new byte[0]));

        assertThat(toArray(collection.find(json("n: {$type: 5}")).sort(json("n: 1"))))
            .containsExactly(
                json("_id: 9").append("n", new Binary(new byte[0])),
                json("_id: 7").append("n", new Binary(new byte[] { 0, 0, 0, 1 })),
                json("_id: 8").append("n", new Binary(highBytes)),
                json("_id: 2").append("n", new UUID(0, 1)),
                json("_id: 3").append("n", new UUID(1, 0))
            );

        assertThat(toArray(collection.find(new Document("n", new UUID(1, 0)))))
            .containsExactly(
                json("_id: 3").append("n", new UUID(1, 0))
            );

        assertThat(toArray(collection.find(json("")).sort(json("n: 1"))))
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
            11000, "E11000 duplicate key error collection: testdb.testcoll index: _id_ dup key: { : BinData(3, 00000000000000000100000000000000) }");

        assertMongoWriteException(() -> collection.insertOne(new Document("_id", new UUID(999999, 128))),
            11000, "E11000 duplicate key error collection: testdb.testcoll index: _id_ dup key: { : BinData(3, 3F420F00000000008000000000000000) }");

        collection.deleteOne(new Document("_id", new UUID(0, 2)));

        assertThat(toArray(collection.find(json(""))))
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

        assertThat(toArray(collection.find(json("_id: {$type: 2.0}"))))
            .containsExactly(json("_id: 'abc'"));

        assertThat(toArray(collection.find(json("_id: {$type: [16, 'string']}"))))
            .containsExactlyInAnyOrder(
                json("_id: 1"),
                json("_id: 'abc'")
            );

        assertThat(toArray(collection.find(json("_id: {$type: 'number'}"))))
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

        assertThat(toArray(collection.find(json("a: {b: {$type: []}}")))).isEmpty();

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

        assertThat(toArray(collection.distinct("a.b", Integer.class)))
            .containsExactly(1, 2, 3, null);

        assertThat(collection.distinct("a.c", Integer.class)).isEmpty();
    }

    @Test
    public void testDropCollection() throws Exception {
        collection.insertOne(json(""));
        assertThat(toArray(db.listCollectionNames())).contains(collection.getNamespace().getCollectionName());
        collection.drop();
        assertThat(toArray(db.listCollectionNames())).doesNotContain(collection.getNamespace().getCollectionName());
    }

    @Test
    public void testDropCollectionAlsoDropsFromDB() throws Exception {
        collection.insertOne(json(""));
        collection.drop();
        assertThat(collection.countDocuments()).isZero();
        assertThat(toArray(db.listCollectionNames())).doesNotContain(collection.getNamespace().getCollectionName());
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
        assertThat(toArray(db.listCollectionNames())).doesNotContain(collection.getNamespace().getCollectionName(),
            collection2.getNamespace().getCollectionName());

        collection.insertOne(json("_id: 1"));
        collection2.insertOne(json("_id: 1"));
    }

    @Test
    public void testEmbeddedSort() {
        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));
        collection.insertOne(json("_id: 3"));
        collection.insertOne(json("_id: 4, counts: {done: 1}"));
        collection.insertOne(json("_id: 5, counts: {done: 2}"));

        List<Document> objs = toArray(collection.find(ne("c", true)).sort(json("\"counts.done\": -1, _id: 1")));
        assertThat(objs).containsExactly(
            json("_id: 5, counts: {done: 2}"),
            json("_id: 4, counts: {done: 1}"),
            json("_id: 1"),
            json("_id: 2"),
            json("_id: 3"));
    }

    @Test
    public void testFindAndModifyCommandEmpty() throws Exception {
        Document cmd = new Document("findandmodify", collection.getNamespace().getCollectionName());

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> db.runCommand(cmd))
            .withMessageContaining("Command failed with error 9 (FailedToParse): 'Either an update or remove=true must be specified'");
    }

    @Test
    public void testFindAndModifyCommandIllegalOp() throws Exception {
        collection.insertOne(json("_id: 1"));

        Document cmd = new Document("findAndModify", collection.getNamespace().getCollectionName());
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

        Document cmd = new Document("findAndModify", collection.getNamespace().getCollectionName());
        cmd.put("query", json("_id: 1"));
        cmd.put("update", json("$inc: {a: 1}"));

        Document result = db.runCommand(cmd);
        assertThat(result.get("lastErrorObject")).isEqualTo(json("updatedExisting: true, n: 1"));
        assertThat(result.getDouble("ok")).isEqualTo(1.0);

        assertThat(collection.find().first()).isEqualTo(json("_id: 1, a: 1"));
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

        List<Document> actual = toArray(collection.find().sort(json("_id: 1")).limit(2));
        assertThat(actual).containsExactly(json("_id: 1"), json("_id: 2"));

        List<Document> actualNegativeLimit = toArray(collection.find().sort(json("_id: 1")).limit(-2));
        assertThat(actualNegativeLimit).isEqualTo(actual);
    }

    @Test
    public void testFindInReverseNaturalOrder() {
        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));

        List<Document> actual = toArray(collection.find().sort(json("$natural: -1")));
        assertThat(actual).containsExactly(
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

        assertThat(toArray(collection.find(new Document("_id", Pattern.compile("mart")))))
            .containsExactly(json("_id: 'marta'"));

        assertThat(toArray(collection.find(new Document("foo", Pattern.compile("ba")))))
            .containsExactly(json("_id: 'john', foo: 'bar'"), json("_id: 'jon', foo: 'ba'"));

        assertThat(toArray(collection.find(new Document("foo", Pattern.compile("ba$")))))
            .containsExactly(json("_id: 'jon', foo: 'ba'"));
    }

    @Test
    public void testFindWithQuery() {
        collection.insertOne(json("name: 'jon'"));
        collection.insertOne(json("name: 'leo'"));
        collection.insertOne(json("name: 'neil'"));
        collection.insertOne(json("name: 'neil'"));

        assertThat(toArray(collection.find(json("name: 'neil'")))).hasSize(2);
    }

    @Test
    public void testFindWithSkipLimit() {
        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));
        collection.insertOne(json("_id: 3"));
        collection.insertOne(json("_id: 4"));

        assertThat(toArray(collection.find().sort(json("_id: 1")).limit(2).skip(2)))
            .containsExactly(json("_id: 3"), json("_id: 4"));
    }

    @Test
    public void testFindWithSkipLimitInReverseOrder() {
        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));
        collection.insertOne(json("_id: 3"));
        collection.insertOne(json("_id: 4"));

        assertThat(toArray(collection.find().sort(json("_id: -1")).limit(2).skip(2)))
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

        assertThat(toArray(collection.find().sort(json("_id: 1")).limit(2).skip(2)))
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
        assertThat(toArray(db.listCollectionNames())).contains("coll");
    }

    @Test
    public void testNullId() throws Exception {
        collection.insertOne(json("_id: null, name: 'test'"));
        Document result = collection.find(json("name: 'test'")).first();
        assertThat(result).isNotNull();
        assertThat(result.getObjectId(Constants.ID_FIELD)).isNull();

        assertMongoWriteException(() -> collection.insertOne(json("_id: null")),
            11000, "E11000 duplicate key error collection: testdb.testcoll index: _id_ dup key: { : null }");

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

        List<Document> docs = toArray(collection.find(json("_id: {$in: [3, 2, 1]}")));
        assertThat(docs).containsExactlyInAnyOrder(json("_id: 1"), json("_id: 2"), json("_id: 3"));
    }

    @Test
    public void testIdNotAllowedToBeUpdated() {
        collection.insertOne(json("_id: 1"));

        assertMongoWriteException(() -> collection.replaceOne(json("_id: 1"), json("_id: 2, a: 4")),
            66, "After applying the update, the (immutable) field '_id' was found to have been altered to _id: 2");

        // test with $set

        assertMongoWriteException(() -> collection.updateOne(json("_id: 1"), new Document("$set", json("_id: 2"))),
            66, "Performing an update on the path '_id' would modify the immutable field '_id'");
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
            11000, "E11000 duplicate key error collection: testdb.testcoll index: _id_ dup key: { : 1.0 }");

        assertThat(collection.countDocuments()).isEqualTo(1);
    }

    @Test(expected = MongoException.class)
    public void testInsertDuplicateThrows() {
        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 1"));
    }

    @Test(expected = MongoException.class)
    public void testInsertDuplicateWithConcernThrows() {
        collection.insertOne(json("_id: 1"));
        collection.withWriteConcern(WriteConcern.ACKNOWLEDGED).insertOne(json("_id: 1"));
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
    public void testMaxBsonSize() throws Exception {
        int maxBsonObjectSize = syncClient.getMaxBsonObjectSize();
        assertThat(maxBsonObjectSize).isEqualTo(16777216);
    }

    @Test
    public void testQuery() throws Exception {
        Document obj = collection.find(json("_id: 1")).first();
        assertThat(obj).isNull();
        assertThat(collection.countDocuments()).isEqualTo(0);
    }

    @Test
    public void testQueryAll() throws Exception {
        List<Object> inserted = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Document obj = new Document("_id", i);
            collection.insertOne(obj);
            inserted.add(obj);
        }
        assertThat(collection.countDocuments()).isEqualTo(10);

        assertThat(toArray(collection.find().sort(json("_id: 1")))).isEqualTo(inserted);
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

        List<Document> objects = toArray(collection.find().sort(json("_id: 1")));
        double before = Double.MIN_VALUE;
        for (Document obj : objects) {
            double value = obj.getDouble("_id").doubleValue();
            assertThat(value).isGreaterThanOrEqualTo(before);
            before = value;
        }

        // reverse sort
        objects = toArray(collection.find().sort(json("_id: -1")));
        before = Double.MAX_VALUE;
        for (Document obj : objects) {
            double value = obj.getDouble("_id").doubleValue();
            assertThat(value).isLessThanOrEqualTo(before);
            before = value;
        }
    }

    @Test
    public void testQueryWithFieldSelector() throws Exception {
        collection.insertOne(json("foo: 'bar'"));
        Document obj = collection.find(json("")).projection(json("foo: 1")).first();
        assertThat(obj.keySet()).containsExactlyInAnyOrder("_id", "foo");

        obj = collection.find(json("foo:'bar'")).projection(json("_id: 1")).first();
        assertThat(obj.keySet()).containsExactly("_id");

        obj = collection.find(json("foo: 'bar'")).projection(json("_id: 0, foo: 1")).first();
        assertThat(obj.keySet()).containsExactly("foo");
    }

    @Test
    public void testQueryWithDotNotationFieldSelector() throws Exception {
        collection.insertOne(json("_id: 123, index: false, foo: {a: 'a1', b: 0}"));
        Document obj = collection.find(json("")).projection(json("'foo.a': 1, 'foo.b': 1")).first();
        assertThat(obj).isEqualTo(json("_id: 123, foo: {a: 'a1', b: 0}"));

        obj = collection.find(json("")).projection(json("'foo.a': 1")).first();
        assertThat(obj).isEqualTo(json("_id: 123, foo: {a: 'a1'}"));

        obj = collection.find(json("")).projection(json("'foo.a': 1, index: 1, _id: 0")).first();
        assertThat(obj).isEqualTo(json("foo: {a: 'a1'}, index: false"));

        obj = collection.find(json("")).projection(json("foo: 1, _id: 0")).first();
        assertThat(obj).isEqualTo(json("foo: {a: 'a1', b: 0}"));

        obj = collection.find(json("")).projection(json("'foo.a.b.c.d': 1")).first();
        assertThat(obj).isEqualTo(json("_id: 123, foo: {}"));
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

        assertThat(toArray(collection.find(json("$and: [{'tags': {$all: ['A']}}, {'tags': {$nin: ['B', 'C']}}]"))))
            .containsExactly(
                json("_id: 2, tags: ['A', 'D']")
            );
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/36
    @Test
    public void testMatchesAllWithEmptyCollection() throws Exception {
        collection.insertOne(json("_id: 1, text: 'TextA', tags: []"));
        collection.insertOne(json("_id: 2, text: 'TextB', tags: []"));
        collection.insertOne(json("_id: 3, text: 'TextA', tags: ['A']"));

        assertThat(toArray(collection.find(json("$and: [{'text': 'TextA'}, {'tags': {$all: []}}]")))).isEmpty();
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
        Date before = new Date();
        Document serverStatus = runCommand("serverStatus");
        assertThat(serverStatus.getDouble("ok")).isEqualTo(1);
        assertThat(serverStatus.get("uptime")).isInstanceOf(Number.class);
        assertThat(serverStatus.get("uptimeMillis")).isInstanceOf(Long.class);
        Date serverTime = (Date) serverStatus.get("localTime");
        assertThat(serverTime).isNotNull();
        assertThat(serverTime.after(new Date())).isFalse();
        assertThat(before.after(serverTime)).isFalse();

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
            assertThat(result.get("you").toString()).startsWith("127.0.0.1:");
        }
    }

    @Test
    public void testSortDocuments() throws Exception {
        collection.insertOne(json("_id: 1, a: {b: 1}"));
        collection.insertOne(json("_id: 2, a: {b: 2}"));
        collection.insertOne(json("_id: 3, a: 3"));
        collection.insertOne(json("_id: 4, a: {c: 1}"));

        assertThat(toArray(collection.find().sort(json("a: 1"))))
            .containsExactly(
                json("_id: 3, a: 3"),
                json("_id: 1, a: {b: 1}"),
                json("_id: 2, a: {b: 2}"),
                json("_id: 4, a: {c: 1}")
            );

        assertThat(toArray(collection.find().sort(json("a: -1"))))
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

        assertThat(toArray(collection.find().sort(json("a: 1, _id: 1"))))
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

        assertThat(toArray(collection.find().sort(json("a: -1, _id: 1"))))
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
        List<Document> results = toArray(collection.find().sort(json("'a.b': -1")));
        assertThat(results).containsExactly(
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
            2, "The positional operator did not find the match needed from the query.");
    }

    @Test
    public void testUpdateMultiplePositional() throws Exception {
        collection.insertOne(json("a: {b: {c: 1}}"));
        assertMongoWriteException(() -> collection.updateOne(json("'a.b.c': 1"), json("$set: {'a.$.b.$.c': 1}")),
            2, "Too many positional (i.e. '$') elements found in path 'a.$.b.$.c'");
    }

    @Test
    public void testUpdateIllegalFieldName() throws Exception {

        // Disallow $ in field names - SERVER-3730

        collection.insertOne(json("x: 1"));

        collection.updateOne(json("x: 1"), json("$set: {y: 1}")); // ok

        assertMongoWriteException(() -> collection.updateOne(json("x: 1"), json("$set: {$z: 1}")),
            15896, "Modified field name may not start with $");

        // unset ok to remove bad fields
        collection.updateOne(json("x: 1"), json("$unset: {$z: 1}"));

        assertMongoWriteException(() -> collection.updateOne(json("x: 1"), json("$inc: {$z: 1}")),
            15896, "Modified field name may not start with $");

        assertMongoWriteException(() -> collection.updateOne(json("x: 1"), json("$pushAll: {$z: [1, 2, 3]}")),
            15896, "Modified field name may not start with $");
    }

    @Test
    public void testUpdateSubdocument() throws Exception {
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> collection.updateOne(json(""), json("'a.b.c': 123")))
            .withMessage("Invalid BSON field name a.b.c");
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
            2, "The field 'field' must be an array but is of type string in document {_id: 1}");

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
    public void testUpdatePushAll() throws Exception {
        Document idObj = json("_id: 1");
        collection.insertOne(idObj);
        assertMongoWriteException(() -> collection.updateOne(idObj, json("$pushAll: {field: 'value'}")),
            10153, "Modifier $pushAll allowed for arrays only");

        collection.updateOne(idObj, json("$pushAll: {field: ['value', 'value2']}"));
        assertThat(collection.find(idObj).first()).isEqualTo(json("_id: 1, field: ['value', 'value2']"));
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
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, a: [6, 5, 4]"));

        collection.updateOne(json("_id: 1"), addEachToSet("a", Arrays.asList(3, 2, 1)));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, a: [6, 5, 4, 3, 2, 1]"));

        collection.updateOne(json("_id: 1"), addEachToSet("a", Arrays.asList(7, 7, 9, 2)));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, a: [6, 5, 4, 3, 2, 1, 7, 9]"));

        collection.updateOne(json("_id: 1"), addEachToSet("a", Arrays.asList(12, 13, 12)));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, a: [6, 5, 4, 3, 2, 1, 7, 9, 12, 13]"));
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
            2, "Cannot apply $pull to a non-array value");

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
            2, "$pullAll requires an array argument but was given a string");

        collection.updateOne(obj, json("$set: {field1: ['value1', 'value2', 'value1', 'value3', 'value4', 'value3']}"));

        collection.updateOne(obj, json("$pullAll: {field1: ['value1', 'value3']}"));

        assertThat(collection.find(obj).first().get("field1")).isEqualTo(Arrays.asList("value2", "value4"));

        assertMongoWriteException(() -> collection.updateOne(obj, json("$pullAll: {field1: 'bar'}")),
            2, "$pullAll requires an array argument but was given a string");
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
            28, "Cannot create field 'bar' in element {foo: \"bar\"}");
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

        assertThat(toArray(collection.find(json(""))))
            .containsExactlyInAnyOrder(
                json("_id: 1, a: [1, 2], b: [1, 2]"),
                json("_id: 2, a: [2, 3], b: [1, 2]"),
                json("_id: 3, a: null, b: null"),
                json("_id: 4, a: 'abc', b: 'xyz'"),
                json("_id: 5, a: 1, b: 2")
            );

        collection.updateMany(json(""), json("$max: {a: [1, 3], b: [2, 3]}"));

        assertThat(toArray(collection.find(json(""))))
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
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss", Locale.US);

        Document object = new Document("_id", 1).append("desc", "crafts")
            .append("dateEntered", df.parse("2013-10-01T05:00:00"))
            .append("dateExpired", df.parse("2013-10-01T16:38:16"));

        collection.insertOne(object);

        collection.updateOne(json("_id: 1"),
            new Document("$max", new Document("dateExpired", df.parse("2013-09-30T00:00:00"))));
        assertThat(collection.find(json("_id: 1")).first())
            .isEqualTo(json("_id: 1, desc: 'crafts'")
                .append("dateEntered", df.parse("2013-10-01T05:00:00"))
                .append("dateExpired", df.parse("2013-10-01T16:38:16")));

        collection.updateOne(json("_id: 1"),
            new Document("$max", new Document("dateExpired", df.parse("2014-01-07T00:00:00"))));
        assertThat(collection.find(json("_id: 1")).first()).isEqualTo(
            json("_id: 1, desc: 'crafts'")
                .append("dateEntered", df.parse("2013-10-01T05:00:00"))
                .append("dateExpired", df.parse("2014-01-07T00:00:00")));
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
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss", Locale.US);

        Document object = new Document("_id", 1).append("desc", "crafts")
            .append("dateEntered", df.parse("2013-10-01T05:00:00"))
            .append("dateExpired", df.parse("2013-10-01T16:38:16"));

        collection.insertOne(object);

        collection.updateOne(json("_id: 1"),
            new Document("$min", new Document("dateEntered", df.parse("2013-09-25T00:00:00"))));
        assertThat(collection.find(json("_id: 1")).first()) //
            .isEqualTo(json("_id: 1, desc: 'crafts'") //
                .append("dateEntered", df.parse("2013-09-25T00:00:00")) //
                .append("dateExpired", df.parse("2013-10-01T16:38:16")));

        collection.updateOne(json("_id: 1"),
            new Document("$min", new Document("dateEntered", df.parse("2014-01-07T00:00:00"))));
        assertThat(collection.find(json("_id: 1")).first()) //
            .isEqualTo(json("_id: 1, desc: 'crafts'") //
                .append("dateEntered", df.parse("2013-09-25T00:00:00")) //
                .append("dateExpired", df.parse("2013-10-01T16:38:16")));
    }

    @Test
    public void testUpdatePop() throws Exception {
        Document object = json("_id: 1");

        collection.insertOne(object);
        collection.updateOne(object, json("$pop: {'foo.bar': 1}"));

        assertThat(collection.find(object).first()).isEqualTo(object);
        collection.updateOne(object, json("$set: {'foo.bar': [1, 2, 3]}"));
        assertThat(collection.find(object).first()).isEqualTo(json("_id: 1, foo: {bar: [1, 2, 3]}"));

        collection.updateOne(object, json("$pop: {'foo.bar': 1}"));
        assertThat(collection.find(object).first()).isEqualTo(json("_id: 1, foo: {bar: [1, 2]}"));

        collection.updateOne(object, json("$pop: {'foo.bar': -1}"));
        assertThat(collection.find(object).first()).isEqualTo(json("_id: 1, foo: {bar: [2]}"));

        assertMongoWriteException(() -> collection.updateOne(object, json("$pop: {'foo.bar': null}")),
            9, "Expected a number in: foo.bar: null");
    }

    @Test
    public void testUpdateUnset() throws Exception {
        Document obj = json("_id: 1, a: 1, b: null, c: 'value'");
        collection.insertOne(obj);
        assertMongoWriteException(() -> collection.updateOne(obj, json("$unset: {_id: ''}")),
            66, "Performing an update on the path '_id' would modify the immutable field '_id'");

        collection.updateOne(obj, json("$unset: {a:'', b:''}"));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, c: 'value'"));

        collection.updateOne(obj, Updates.unset("c.y"));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, c: 'value'"));

        collection.replaceOne(json("_id: 1"), json("a: {b: 'foo', c: 'bar'}"));

        collection.updateOne(json("_id: 1"), json("$unset: {'a.b':1}"));
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
            14, "Cannot apply $inc to a value of non-numeric type. {_id: 1} has the field 'a' of non-numeric type object");

        assertThatExceptionOfType(MongoServerException.class)
            .isThrownBy(() -> collection.updateOne(json("_id: 1"), json("$inc: {'a.x': 'b'}")))
            .withMessage("Cannot increment with non-numeric argument: {a.x: \"b\"}");
    }

    @Test
    public void testUpdateWithIdInMulti() {
        collection.insertMany(Arrays.asList(json("_id: 1"), json("_id: 2")));
        collection.updateMany(json("_id: {$in: [1, 2]}"), json("$set: {n: 1}"));
        List<Document> results = toArray(collection.find());
        assertThat(results).containsExactly(json("_id: 1, n: 1"), json("_id: 2, n: 1"));
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
        List<Document> results = toArray(collection.find());
        assertThat(results).containsExactly(json("_id: 1"), json("_id: 2, n: 1"));
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

        collection.insertOne(json("_id: 1, a: [{x: 1, y: 1}, {x: 2,y: 2}, {x: 3, y: 3}]"));

        collection.updateOne(json("'a.x': 2"), json("$inc: {'a.$.y': 1}"));

        assertThat(collection.find(json("'a.x': 2")).first()).isEqualTo(json("_id: 1, a: [{x: 1, y: 1}, {x: 2, y: 3}, {x: 3, y: 3}]"));

        collection.insertOne(json("'array': [{'123a': {'name': 'old'}}]"));
        assertThat(collection.find(json("'array.123a.name': 'old'")).first()).isNotNull();
        collection.updateOne(json("'array.123a.name': 'old'"), json("$set: {'array.$.123a.name': 'new'}"));
        assertThat(collection.find(json("'array.123a.name': 'new'")).first()).isNotNull();
        assertThat(collection.find(json("'array.123a.name': 'old'")).first()).isNull();
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/32
    @Test
    public void testUpdateWithNotAndSizeOperator() throws Exception {
        collection.insertOne(json("_id: 1, array: ['a', 'b']"));
        collection.insertOne(json("_id: 2, array: ['b']"));
        collection.insertOne(json("_id: 3, array: ['a']"));

        collection.updateMany(json("array: {$not: {$size: 1}}"), json("$pull: {array: 'a'}"));

        assertThat(toArray(collection.find()))
            .containsExactlyInAnyOrder(
                json("_id: 1, array: ['b']"),
                json("_id: 2, array: ['b']"),
                json("_id: 3, array: ['a']")
            );
    }

    @Test
    public void testMultiUpdateArrayMatch() throws Exception {
        collection.insertOne(json(""));
        collection.insertOne(json("x: [1, 2, 3]"));
        collection.insertOne(json("x: 99"));

        collection.updateMany(json("x: 2"), json("$inc: {'x.$': 1}"));
        assertThat(collection.find(json("x: 1")).first().get("x")).isEqualTo(Arrays.asList(1, 3, 3));
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

        assertThat(toArray(collection.find()))
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

        assertThat(toArray(collection.find()))
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
            66, "Performing an update on the path '_id' would modify the immutable field '_id'");

        assertMongoWriteException(() -> collection.updateOne(object, json("$mul: {foo: 2}")),
            14, "Cannot apply $mul to a value of non-numeric type. {_id: 1} has the field 'foo' of non-numeric type string");

        assertMongoWriteException(() -> collection.updateOne(object, json("$mul: {bar: 'x'}")),
            14, "Cannot multiply with non-numeric argument: {bar: \"x\"}");
    }

    @Test
    public void testIsMaster() throws Exception {
        Document isMaster = db.runCommand(new Document("isMaster", Integer.valueOf(1)));
        assertThat(isMaster.getBoolean("ismaster")).isTrue();
        assertThat(isMaster.getDate("localTime")).isInstanceOf(Date.class);
        assertThat(isMaster.getInteger("maxBsonObjectSize")).isGreaterThan(1000);
        assertThat(isMaster.getInteger("maxMessageSizeBytes")).isGreaterThan(isMaster.getInteger("maxBsonObjectSize"));
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
        List<Document> objs = toArray(collection.find(json("group: null")));
        assertThat(objs).as("should have two neils (neil2, neil3)").hasSize(2);

        objs = toArray(collection.find(exists("group", false)));
        assertThat(objs).as("should have one neils (neil3)").hasSize(1);

        // same check but for fields which do not exist in DB
        objs = toArray(collection.find(json("other: null")));
        assertThat(objs).as("should return all documents").hasSize(5);

        objs = toArray(collection.find(exists("other", false)));
        assertThat(objs).as("should return all documents").hasSize(5);
    }

    @Test
    public void testInsertWithIllegalId() throws Exception {
        assertMongoWriteException(() -> collection.insertOne(json("_id: [1, 2, 3]")),
            2, "can't use an array for _id");
    }

    @Test
    public void testInsertsWithUniqueIndex() {
        collection.createIndex(new Document("uniqueKeyField", 1), new IndexOptions().unique(true));

        collection.insertOne(json("uniqueKeyField: 'abc1', afield: 'avalue'"));
        collection.insertOne(json("uniqueKeyField: 'abc2', afield: 'avalue'"));
        collection.insertOne(json("uniqueKeyField: 'abc3', afield: 'avalue'"));

        assertMongoWriteException(() -> collection.insertOne(json("uniqueKeyField: 'abc2', afield: 'avalue'")),
            11000, "E11000 duplicate key error collection: testdb.testcoll index: uniqueKeyField_1 dup key: { : \"abc2\" }");

        collection.insertOne(json("uniqueKeyField: 1"));
        collection.insertOne(json("uniqueKeyField: 1.1"));

        assertMongoWriteException(() -> collection.insertOne(json("uniqueKeyField: 1.0")),
            11000, "E11000 duplicate key error collection: testdb.testcoll index: uniqueKeyField_1 dup key: { : 1.0 }");
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
            11000, "E11000 duplicate key error collection: testdb.testcoll index: action.actionId_1 dup key: { : 1.0 }");

        assertThat(toArray(collection.find(json("action: 'abc1'"))))
            .containsExactly(json("_id: 1, action: 'abc1'"));

        assertThat(toArray(collection.find(json("'action.actionId': 2"))))
            .containsExactly(json("_id: 3, action: {actionId: 2}"));

        assertThat(toArray(collection.find(json("action: {actionId: 2}"))))
            .containsExactly(json("_id: 3, action: {actionId: 2}"));

        assertThat(toArray(collection.find(json("'action.actionId.subKey': 23")))).isEmpty();
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
            11000, "E11000 duplicate key error collection: testdb.testcoll index: a_1 dup key: { : { b: 0 } }");

        assertMongoWriteException(() -> collection.insertOne(json("a: {b: 0.00}")),
            11000, "E11000 duplicate key error collection: testdb.testcoll index: a_1 dup key: { : { b: 0.0 } }");

        assertMongoWriteException(() -> collection.insertOne(json("a: {b: -0.0}")),
            11000, "E11000 duplicate key error collection: testdb.testcoll index: a_1 dup key: { : { b: -0.0 } }");

        assertMongoWriteException(() -> collection.insertOne(json("a: {b: {c: 1.0}}")),
            11000, "E11000 duplicate key error collection: testdb.testcoll index: a_1 dup key: { : { b: { c: 1.0 } } }");

        assertMongoWriteException(() -> collection.insertOne(json("a: {b: {c: 1, d: 1.0}}")),
            11000, "E11000 duplicate key error collection: testdb.testcoll index: a_1 dup key: { : { b: { c: 1, d: 1.0 } } }");
    }

    // see https://github.com/bwaldvogel/mongo-java-server/issues/39
    @Test
    public void testSecondaryUniqueIndexUpdate() throws Exception {
        collection.createIndex(json("text: 1"), new IndexOptions().unique(true));

        collection.insertOne(json("_id: 1, text: 'abc'"));
        collection.insertOne(json("_id: 2, text: 'def'"));
        collection.insertOne(json("_id: 3"));

        assertMongoWriteException(() -> collection.insertOne(json("_id: 4")),
            11000, "E11000 duplicate key error collection: testdb.testcoll index: text_1 dup key: { : null }");

        assertMongoWriteException(() -> collection.updateOne(json("_id: 1"), new Document("$set", json("text: 'def'"))),
            11000, "E11000 duplicate key error collection: testdb.testcoll index: text_1 dup key: { : \"def\" }");

        collection.updateOne(json("_id: 1"), new Document("$set", json("text: 'xyz'")));
        collection.updateOne(json("_id: 2"), new Document("$set", json("text: 'abc'")));

        assertMongoWriteException(() -> collection.updateOne(json("_id: 2"), new Document("$set", json("text: null"))),
            11000, "E11000 duplicate key error collection: testdb.testcoll index: text_1 dup key: { : null }");

        collection.deleteOne(json("text: 'xyz'"));

        assertThat(toArray(collection.find()))
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
            11000, "E11000 duplicate key error collection: testdb.testcoll index: text_1 dup key: { : null }");

        assertMongoWriteException(() -> collection.insertOne(json("_id: 7, text: 'abc'")),
            11000, "E11000 duplicate key error collection: testdb.testcoll index: text_1 dup key: { : \"abc\" }");

        assertMongoWriteException(() -> collection.updateOne(json("_id: 2"), new Document("$set", json("text: null"))),
            11000, "E11000 duplicate key error collection: testdb.testcoll index: text_1 dup key: { : null }");

        collection.deleteOne(json("_id: 5"));

        collection.updateOne(json("_id: 2"), new Document("$set", json("text: null")));
        collection.updateOne(json("_id: 1"), new Document("$set", json("text: 'def'")));

        collection.deleteMany(json("text: null"));

        assertThat(toArray(collection.find())).containsExactly(json("_id: 1, text: 'def'"));
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
            11000, "E11000 duplicate key error collection: testdb.testcoll index: a_1_b_1 dup key: { : null, : null }");

        assertMongoWriteException(() -> collection.insertOne(json("_id: 7, b: null")),
            11000, "E11000 duplicate key error collection: testdb.testcoll index: a_1_b_1 dup key: { : null, : null }");

        collection.deleteMany(json("a: null, b: null"));

        assertThat(toArray(collection.find()))
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
            11000, "E11000 duplicate key error collection: testdb.testcoll index: a.x_1_b.x_1 dup key: { : 1.0, : 2.0 }");

        assertMongoWriteException(() -> collection.insertOne(json("_id: 11, a: {x: null}, b: {x: null}")),
            11000, "E11000 duplicate key error collection: testdb.testcoll index: a.x_1_b.x_1 dup key: { : null, : null }");

        collection.deleteMany(json("a: {x: null}, b: {x: null}"));
        collection.deleteMany(json("a: 10"));
        collection.deleteMany(json("b: 20"));

        assertThat(toArray(collection.find()))
            .containsExactlyInAnyOrder(
                json("_id: 4, a: {x: 1}"),
                json("_id: 5, b: {x: 2}"),
                json("_id: 6, a: {x: 1}, b: {x: 2}"),
                json("_id: 7, a: {x: 2}, b: {x: 2}"),
                json("_id: 9")
            );
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
            11000, "E11000 duplicate key error collection: testdb.testcoll index: a.b.c_1 dup key: { : 1 }");

        assertMongoWriteException(() -> collection.insertOne(json("a: {b: {c: null}}")),
            11000, "E11000 duplicate key error collection: testdb.testcoll index: a.b.c_1 dup key: { : null }");

        assertMongoWriteException(() -> collection.insertOne(json("a: {b: {c: 1, x: 100}}")),
            11000, "E11000 duplicate key error collection: testdb.testcoll index: a.b.c_1 dup key: { : 1 }");

        assertMongoWriteException(() -> collection.insertOne(json("a: {b: {c: {d: 1}}}")),
            11000, "E11000 duplicate key error collection: testdb.testcoll index: a.b.c_1 dup key: { : { d: 1 } }");

        assertMongoWriteException(() -> collection.insertOne(json("a: {b: {c: {d: null}}}")),
            11000, "E11000 duplicate key error collection: testdb.testcoll index: a.b.c_1 dup key: { : { d: null } }");
    }

    @Test
    public void testAddNonUniqueIndexOnNonIdField() {
        collection.insertOne(json("someField: 'abc'"));
        assertThat(toArray(collection.listIndexes())).hasSize(1);

        collection.createIndex(new Document("someField", 1), new IndexOptions().unique(false));
        assertThat(toArray(collection.listIndexes())).hasSize(2);

        collection.insertOne(json("someField: 'abc'"));
    }

    @Test
    public void testAddSparseIndexOnNonIdField() {
        collection.insertOne(json("someField: 'abc'"));
        assertThat(toArray(collection.listIndexes())).hasSize(1);

        collection.createIndex(new Document("someField", 1), new IndexOptions().sparse(true));
        assertThat(toArray(collection.listIndexes())).hasSize(2);

        collection.insertOne(json("someField: 'abc'"));
    }

    @Test
    public void testAddPartialIndexOnNonIdField() {
        collection.insertOne(json("someField: 'abc'"));
        assertThat(toArray(collection.listIndexes())).hasSize(1);

        collection.createIndex(new Document("someField", 1), new IndexOptions()
            .partialFilterExpression(json("someField: {$gt: 5}")));

        assertThat(toArray(collection.listIndexes())).hasSize(2);

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
            11000, "E11000 duplicate key error collection: testdb.testcoll index: a_1_b_1 dup key: { : \"foo\", : \"foo\" }");

        assertMongoWriteException(() -> collection.insertOne(json("b: 'foo'")),
            11000, "E11000 duplicate key error collection: testdb.testcoll index: a_1_b_1 dup key: { : null, : \"foo\" }");

        assertMongoWriteException(() -> collection.insertOne(json("a: {x: 1, y: 1}, b: 'foo'")),
            11000, "E11000 duplicate key error collection: testdb.testcoll index: a_1_b_1 dup key: { : { x: 1, y: 1 }, : \"foo\" }");

        assertThat(toArray(collection.find(json("a: 'bar'"))))
            .containsExactly(json("_id: 6, a: 'bar', b: 'foo'"));

        assertThat(toArray(collection.find(json("b: 'foo', a: 'bar'"))))
            .containsExactly(json("_id: 6, a: 'bar', b: 'foo'"));

        assertThat(toArray(collection.find(json("a: 'foo'"))))
            .containsExactlyInAnyOrder(
                json("_id: 2, a: 'foo'"),
                json("_id: 4, a: 'foo', b: 'foo'"),
                json("_id: 5, a: 'foo', b: 'bar'")
            );
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
            2, "null is not valid type for $currentDate. Please use a boolean ('true') or a $type expression ({$type: 'timestamp/date'}).");

        assertMongoWriteException(() -> collection.updateOne(object, json("$currentDate: {lastModified: 123.456}")),
            2, "double is not valid type for $currentDate. Please use a boolean ('true') or a $type expression ({$type: 'timestamp/date'}).");

        assertMongoWriteException(() -> collection.updateOne(object, json("$currentDate: {lastModified: 'foo'}")),
            2, "string is not valid type for $currentDate. Please use a boolean ('true') or a $type expression ({$type: 'timestamp/date'}).");

        assertMongoWriteException(() -> collection.updateOne(object, json("$currentDate: {lastModified: {$type: 'foo'}}")),
            2, "The '$type' string field is required to be 'date' or 'timestamp': {$currentDate: {field : {$type: 'date'}}}");

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
    }

    @Test
    public void testRenameFieldIllegalValue() throws Exception {
        Document object = json("_id: 1, foo: 'x', bar: 'y'");
        collection.insertOne(object);

        assertMongoWriteException(() -> collection.updateOne(json("_id: 1"), json("$rename: {foo: 12345}")),
            2, "The 'to' field for $rename must be a string: foo: 12345");

        assertMongoWriteException(() -> collection.updateOne(json("_id: 1"), json("$rename: {'_id': 'id'}")),
            66, "Performing an update on the path '_id' would modify the immutable field '_id'");

        assertMongoWriteException(() -> collection.updateOne(json("_id: 1"), json("$rename: {foo: '_id'}")),
            66, "Performing an update on the path '_id' would modify the immutable field '_id'");

        assertMongoWriteException(() -> collection.updateOne(json("_id: 1"), json("$rename: {foo: 'bar', 'bar': 'bar2'}")),
            40, "Updating the path 'bar' would create a conflict at 'bar'");

        assertMongoWriteException(() -> collection.updateOne(json("_id: 1"), json("$rename: {bar: 'foo', bar2: 'foo'}")),
            40, "Updating the path 'foo' would create a conflict at 'foo'");
    }

    @Test
    public void testRenameCollection() throws Exception {
        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));
        collection.insertOne(json("_id: 3"));

        collection.renameCollection(new MongoNamespace(collection.getNamespace().getDatabaseName(), "other-collection-name"));

        Collection<String> collectionNames = toArray(db.listCollectionNames());
        assertThat(collectionNames).containsExactly("other-collection-name");

        assertThat(getCollection("other-collection-name").countDocuments()).isEqualTo(3);
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

        List<String> collectionNames = toArray(db.listCollectionNames());
        assertThat(collectionNames).containsExactlyInAnyOrder(collection.getNamespace().getCollectionName(),
            "other-collection-name");

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

        List<String> collectionNames = toArray(db.listCollectionNames());
        assertThat(collectionNames).containsExactly("other-collection-name");

        assertThat(getCollection("other-collection-name").countDocuments()).isEqualTo(3);
    }

    @Test
    public void testListIndexes_empty() throws Exception {
        assertThat(collection.listIndexes()).isEmpty();
    }

    @Test
    public void testListIndexes() throws Exception {
        collection.insertOne(json("_id: 1"));
        db.getCollection("other").insertOne(json("_id: 1"));

        collection.createIndex(json("bla: 1"));

        collection.createIndex(new Document("a", 1), new IndexOptions().unique(true));
        collection.createIndex(new Document("a", 1).append("b", -1.0), new IndexOptions().unique(true));

        List<Document> indexInfo = toArray(collection.listIndexes());
        assertThat(indexInfo).containsExactlyInAnyOrder(
            json("name: '_id_', ns: 'testdb.testcoll', key: {_id: 1}"),
            json("name: '_id_', ns: 'testdb.other', key: {_id: 1}"),
            json("name: 'bla_1', ns: 'testdb.testcoll', key: {bla: 1}"),
            json("name: 'a_1', ns: 'testdb.testcoll', key: {a: 1}, unique: true"),
            json("name: 'a_1_b_-1', ns: 'testdb.testcoll', key: {a: 1, b: -1.0}, unique: true")
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
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        collection.insertOne(new Document("_id", 1).append("published", true).append("startDate", dateFormat.parse("2015-03-01 13:20:05")));
        collection.insertOne(new Document("_id", 2).append("published", true).append("expiration", dateFormat.parse("2020-12-31 18:00:00")));
        collection.insertOne(new Document("_id", 3).append("published", true));
        collection.insertOne(new Document("_id", 4).append("published", false));
        collection.insertOne(new Document("_id", 5).append("published", true).append("startDate", dateFormat.parse("2017-01-01 00:00:00")));
        collection.insertOne(new Document("_id", 6).append("published", true).append("expiration", dateFormat.parse("2016-01-01 00:00:00")));

        Date now = dateFormat.parse("2016-01-01 00:00:00");
        Bson query = and(
            ne("published", false),
            or(exists("startDate", false), lt("startDate", now)),
            or(exists("expiration", false), gt("expiration", now))
        );
        List<Document> documents = toArray(collection.find(query).projection(json("_id: 1")));
        assertThat(documents).containsExactly(json("_id: 1"), json("_id: 2"), json("_id: 3"));
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
        List<Document> results = toArray(collection.find(inQueryWithNull).projection(json("_id: 1")));
        assertThat(results).containsExactly(
            json("_id: 2"),
            json("_id: 3"),
            json("_id: 5")
        );
    }

    @Test
    public void testQueryWithReference() throws Exception {
        collection.insertOne(json("_id: 1"));
        String collectionName = collection.getNamespace().getCollectionName();
        collection.insertOne(new Document("_id", 2).append("ref", new DBRef(collectionName, 1)));
        collection.insertOne(new Document("_id", 3).append("ref", new DBRef(collectionName, 2)));

        Document doc = collection.find(new Document("ref", new DBRef(collectionName, 1))).projection(json("_id: 1")).first();
        assertThat(doc).isEqualTo(json("_id: 2"));
    }

    @Test
    public void testQueryWithIllegalReference() throws Exception {
        collection.insertOne(json("_id: 1"));
        String collectionName = collection.getNamespace().getCollectionName();
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

        List<Document> documents = toArray(collection.find(json("tags: {$all: ['appliance', 'school', 'book']}")));
        assertThat(documents)
            .extracting(d -> d.get("_id"))
            .containsExactly(new ObjectId("5234cc89687ea597eabee675"), new ObjectId("5234cc8a687ea597eabee676"));
    }

    @Test
    public void testMatchesElementQuery() throws Exception {
        collection.insertOne(json("_id: 1, results: [82, 85, 88]"));
        collection.insertOne(json("_id: 2, results: [75, 88, 89]"));

        assertThat(toArray(collection.find(json("results: {$elemMatch: {$gte: 80, $lt: 85}}"))))
            .containsExactly(
                json("_id: 1, results: [82, 85, 88]")
            );
    }

    @Test
    public void testMatchesElementInEmbeddedDocuments() throws Exception {
        collection.insertOne(json("_id: 1, results: [{product: 'abc', score: 10}, {product: 'xyz', score: 5}]"));
        collection.insertOne(json("_id: 2, results: [{product: 'abc', score:  9}, {product: 'xyz', score: 7}]"));
        collection.insertOne(json("_id: 3, results: [{product: 'abc', score:  7}, {product: 'xyz', score: 8}]"));

        assertThat(toArray(collection.find(json("results: {$elemMatch: {product: 'xyz', score: {$gte: 8}}}"))))
            .containsExactlyInAnyOrder(
                json("_id: 3, results: [{product: 'abc', score:  7}, {product: 'xyz', score: 8}]")
            );

        assertThat(toArray(collection.find(json("results: {$elemMatch: {product: 'xyz'}}}"))))
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

    @Test
    public void testMatchesNullOrMissing() throws Exception {
        collection.insertOne(json("_id: 1, x: null"));
        collection.insertOne(json("_id: 2"));
        collection.insertOne(json("_id: 3, x: 123"));

        assertThat(toArray(collection.find(json("x: null"))))
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

        assertThat(toArray(collection.find(json("x: {y: 23, $lt: 10}")))).isEmpty();
        assertThat(toArray(collection.find(json("x: {y: {$lt: 100, z: 23}}")))).isEmpty();
        assertThat(toArray(collection.find(json("a: 123, x: {y: {$lt: 100, z: 23}}")))).isEmpty();
    }

    @Test
    public void testQueryWithComment() throws Exception {
        collection.insertOne(json("_id: 1, x: 2"));
        collection.insertOne(json("_id: 2, x: 3"));
        collection.insertOne(json("_id: 3, x: 4"));

        List<Document> documents = toArray(collection.find(json("x: {$mod: [2, 0 ]}, $comment: \"Find even values.\"")));
        assertThat(documents).extracting(d -> d.get("_id")).containsExactly(1, 3);
    }

    @Test
    public void testValidate() throws Exception {
        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> db.runCommand(new Document("validate", collection.getNamespace().getCollectionName())))
            .withMessageContaining("Command failed with error 26 (NamespaceNotFound): 'ns not found'");

        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));
        collection.insertOne(json("_id: 3"));

        collection.deleteOne(json("_id: 2"));

        Document result = db.runCommand(new Document("validate", collection.getNamespace().getCollectionName()));
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
            .withMessageContaining("E11000 duplicate key error collection: testdb.testcoll index: _id_ dup key: { : 1.0 }");

        Document lastError = db.runCommand(json("getlasterror: 1"));
        assertThat(lastError.get("code")).isEqualTo(11000);
        assertThat(lastError.getString("err")).contains("duplicate key");
        assertThat(lastError.get("ok")).isEqualTo(1.0);
    }

    @Test
    public void testGetPrevError() throws Exception {
        restart();

        collection.insertOne(json("_id: 1"));

        assertThat(db.runCommand(json("getpreverror: 1")))
            .isEqualTo(json("n: 0, nPrev: -1, err: null, ok: 1.0"));

        assertMongoWriteException(() -> collection.insertOne(json("_id: 1.0")),
            11000, "E11000 duplicate key error collection: testdb.testcoll index: _id_ dup key: { : 1.0 }");

        Document lastError = db.runCommand(json("getpreverror: 1"));
        assertThat(lastError.get("code")).isEqualTo(11000);
        assertThat(lastError.getString("err")).contains("duplicate key");
        assertThat(lastError.getDouble("ok")).isEqualTo(1.0);
    }

    @Test
    public void testResetError() throws Exception {
        collection.insertOne(json("_id: 1"));

        assertThatExceptionOfType(MongoWriteException.class)
            .isThrownBy(() -> collection.insertOne(json("_id: 1.0")))
            .withMessageContaining("duplicate key error collection: testdb.testcoll index: _id_ dup key: { : 1.0 }");

        assertThat(db.runCommand(json("reseterror: 1")))
            .isEqualTo(json("ok: 1.0"));

        assertThat(db.runCommand(json("getpreverror: 1")))
            .isEqualTo(json("nPrev: -1, err: null, n: 0, ok: 1.0"));

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

        assertThat(toArray(collection.find(query))).isEmpty();

        collection.insertOne(json("_id: 1, category: 'food', budget: 400, spent: 450"));
        collection.insertOne(json("_id: 2, category: 'drinks', budget: 100, spent: 150"));
        collection.insertOne(json("_id: 3, category: 'clothes', budget: 100, spent: 50"));
        collection.insertOne(json("_id: 4, category: 'misc', budget: 500, spent: 300"));
        collection.insertOne(json("_id: 5, category: 'travel', budget: 200, spent: 650"));

        assertThat(toArray(collection.find(query)))
            .containsExactly(
                json("_id: 1, category: 'food', budget: 400, spent: 450"),
                json("_id: 2, category: 'drinks', budget: 100, spent: 150"),
                json("_id: 5, category: 'travel', budget: 200, spent: 650")
            );

        assertThat(toArray(collection.find(json("_id: {$gt: 3}")))).hasSize(2);
        assertThat(toArray(collection.find(json("_id: {$gt: {$expr: {$literal: 3}}}")))).isEmpty();
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

        assertThat(toArray(collection.find(json("'b.c': 1"))))
            .containsExactlyInAnyOrder(
                json("_id: 5, b: {c: [1, 2, 3]}"),
                json("_id: 7, b: {c: 1, d: 2}")
            );

        assertThat(toArray(collection.find(json("b: {c: 1}")))).isEmpty();

        assertThat(toArray(collection.find(json("'b.c': null"))))
            .containsExactlyInAnyOrder(
                json("_id: 1, b: null"),
                json("_id: 2, b: {c: null}"),
                json("_id: 4, b: {c: ['a', null, 'b']}"),
                json("_id: 6")
            );

        assertThat(toArray(collection.find(json("b: {c: null}"))))
            .containsExactly(json("_id: 2, b: {c: null}"));

        assertThat(toArray(collection.find(json("'b.c': {d: 1}")))).isEmpty();

        assertThat(toArray(collection.find(json("'b.c': {d: {$gte: 1}}")))).isEmpty();
        assertThat(toArray(collection.find(json("'b.c': {d: {$gte: 1}, e: {$lte: 2}}")))).isEmpty();

        assertThat(toArray(collection.find(json("'b.c.d': {$gte: 1}"))))
            .containsExactlyInAnyOrder(json("_id: 8, b: {c: {d: 1, e: 2}}"));

        assertThat(toArray(collection.find(json("'b.c': {d: 1, e: 2}"))))
            .containsExactlyInAnyOrder(json("_id: 8, b: {c: {d: 1, e: 2}}"));

        assertThat(toArray(collection.find(json("'b.c.e': 2"))))
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

        assertThat(toArray(collection.find(json("a: {b: 1.0, c: -0.0}"))))
            .containsExactlyInAnyOrder(
                json("_id: 1, a: {b: 1, c: 0}"),
                json("_id: 2, a: {b: 1, c: 0.0}"),
                json("_id: 3, a: {b: 1.0, c: 0.0}"),
                json("_id: 4, a: {b: 1.0, c: 0}")
            );

        assertThat(toArray(collection.find(json("a: {b: {c: 1}}"))))
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

        assertThat(toArray(collection.find(json("")).sort(json("a: 1, _id: 1"))))
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

        assertThat(toArray(collection.find(json("")).sort(json("a: 1, _id: 1"))))
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

        assertThat(toArray(collection.find(json("a: [2, 1]"))))
            .containsExactlyInAnyOrder(
                json("_id: 1, a: [2, 1]"),
                json("_id: 2, a: [2, 1.0]")
            );

        assertThat(toArray(collection.find(json("a: [1, 2]"))))
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

        assertThat(toArray(collection.find(json("")).sort(json("a: 1, _id: -1"))))
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

        assertThat(toArray(collection.find(json("")).sort(json("a: 1, _id: 1"))))
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

        assertThat(toArray(collection.find(json("")).sort(json("a: -1, _id: -1"))))
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

        assertThat(toArray(collection.distinct("a", Document.class)))
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

        List<Document> documents = toArray(collection.find(json("ref: {$ref: 'coll1', $id: 1}")).projection(json("_id: 1")));
        assertThat(documents).containsExactly(json("_id: 1"));
    }

    @Test
    public void testInsertAndQueryNegativeZero() throws Exception {
        collection.insertOne(json("_id: 1, value: -0.0"));
        collection.insertOne(json("_id: 2, value: 0.0"));
        collection.insertOne(json("_id: 3, value: -0.0"));

        assertThat(toArray(collection.find(json("value: -0.0"))))
            .containsExactlyInAnyOrder(
                json("_id: 1, value: -0.0"),
                json("_id: 2, value: 0.0"),
                json("_id: 3, value: -0.0")
            );

        assertThat(toArray(collection.find(json("value: {$lt: 0.0}")))).isEmpty();

        assertThat(toArray(collection.find(json("value: 0")).sort(json("value: 1, _id: 1"))))
            .extracting(doc -> doc.getDouble("value"))
            .containsExactly(-0.0, +0.0, -0.0);
    }

    @Test
    public void testUniqueIndexWithNegativeZero() throws Exception {
        collection.createIndex(json("value: 1"), new IndexOptions().unique(true));

        collection.insertOne(json("_id: 1, value: -0.0"));

        assertMongoWriteException(() -> collection.insertOne(json("_id: 2, value: 0.0")),
            11000, "E11000 duplicate key error collection: testdb.testcoll index: value_1 dup key: { : 0.0 }");

        assertMongoWriteException(() -> collection.insertOne(json("_id: 3, value: -0.0")),
            11000, "E11000 duplicate key error collection: testdb.testcoll index: value_1 dup key: { : -0.0 }");

        assertMongoWriteException(() -> collection.insertOne(json("_id: 4, value: 0")),
            11000, "E11000 duplicate key error collection: testdb.testcoll index: value_1 dup key: { : 0 }");
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/45
    @Test
    public void testDecimal128() throws Exception {
        collection.insertOne(json("_id: {'$numberDecimal': '1'}"));
        collection.insertOne(json("_id: {'$numberDecimal': '2'}"));
        collection.insertOne(json("_id: {'$numberDecimal': '3.0'}"));
        collection.insertOne(json("_id: {'$numberDecimal': '200000000000000000000000000000000.5'}"));

        assertMongoWriteException(() -> collection.insertOne(json("_id: {'$numberDecimal': '1'}")),
            11000, "E11000 duplicate key error collection: testdb.testcoll index: _id_ dup key: { : 1 }");

        assertThat(toArray(collection.find(json("_id: {$eq: {'$numberDecimal': '3'}}"))))
            .containsExactly(
                json("_id: {'$numberDecimal': '3.0'}")
            );

        assertThat(toArray(collection.find(json("_id: {$gt: {'$numberDecimal': '100000'}}"))))
            .containsExactly(
                json("_id: {'$numberDecimal': '200000000000000000000000000000000.5'}")
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

        assertThat(toArray(collection.find(json("values: {$ne: 0}"))))
            .containsExactly(
                json("_id: 'a', values: [-1]"),
                json("_id: 'c', values: 1.0"),
                json("_id: 'd', values: {'$numberDecimal': '1.0'}")
            );
    }

    private void insertAndFindLargeDocument(int numKeyValues, int id) {
        Document document = new Document("_id", id);
        for (int i = 0; i < numKeyValues; i++) {
            document.put("key-" + i, "value-" + i);
        }
        collection.insertOne(document);

        Document persistentDocument = collection.find(new Document("_id", id)).first();
        assertThat(persistentDocument.keySet()).hasSize(numKeyValues + 1);
    }

    private void insertUpdateInBulk(boolean ordered) {
        List<WriteModel<Document>> ops = new ArrayList<>();

        ops.add(new InsertOneModel<>(json("_id: 1, field: 'x'")));
        ops.add(new InsertOneModel<>(json("_id: 2, field: 'x'")));
        ops.add(new InsertOneModel<>(json("_id: 3, field: 'x'")));
        ops.add(new UpdateManyModel<Document>(json("field: 'x'"), set("field", "y")));

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
        ops.add(new UpdateOneModel<Document>(ne("foo", "bar"), set("field", "y")));

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
    private interface Callable {
        void call();
    }

    private static void assertMongoWriteException(Callable callable, int expectedErrorCode, String expectedMessage) {
        try {
            callable.call();
            fail("MongoWriteException expected");
        } catch (MongoWriteException e) {
            assertThat(e).hasMessage(expectedMessage);
            assertThat(e.getError().getCode()).isEqualTo(expectedErrorCode);
        }
    }

}
