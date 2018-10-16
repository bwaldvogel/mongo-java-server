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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.InetSocketAddress;
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

import org.bson.BsonObjectId;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DBRef;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoQueryException;
import com.mongodb.MongoWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.MongoClients;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.DeleteManyModel;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.RenameCollectionOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.MongoServer;

public abstract class AbstractBackendTest {

    private static final Logger log = LoggerFactory.getLogger(AbstractBackendTest.class);

    protected static final String TEST_DATABASE_NAME = "testdb";
    protected static final String OTHER_TEST_DATABASE_NAME = "bar";

    private MongoServer mongoServer;

    protected com.mongodb.MongoClient syncClient;
    private com.mongodb.async.client.MongoClient asyncClient;

    protected MongoDatabase db;
    protected MongoCollection<Document> collection;

    private com.mongodb.async.client.MongoCollection<Document> asyncCollection;

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

    protected abstract MongoBackend createBackend() throws Exception;

    @Before
    public void setUp() throws Exception {
        spinUpServer();
    }

    @After
    public void tearDown() {
        shutdownServer();
    }

    protected void spinUpServer() throws Exception {
        MongoBackend backend = createBackend();
        mongoServer = new MongoServer(backend);
        InetSocketAddress serverAddress = mongoServer.bind();
        syncClient = new com.mongodb.MongoClient(new ServerAddress(serverAddress));
        asyncClient = MongoClients.create("mongodb://" + serverAddress.getHostName() + ":" + serverAddress.getPort());
        db = syncClient.getDatabase(TEST_DATABASE_NAME);
        collection = db.getCollection("testcoll");

        MongoNamespace namespace = collection.getNamespace();
        com.mongodb.async.client.MongoDatabase asyncDb = asyncClient.getDatabase(namespace.getDatabaseName());
        asyncCollection = asyncDb.getCollection(namespace.getCollectionName());
    }

    protected void shutdownServer() {
        syncClient.close();
        asyncClient.close();
        mongoServer.shutdownNow();
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
        assertThat(toArray(db.listCollectionNames()).contains(newCollectionName));
    }

    @Test
    public void testCreateCollectionAlreadyExists() throws Exception {
        db.createCollection("some-collection", new CreateCollectionOptions());
        try {
            db.createCollection("some-collection", new CreateCollectionOptions());
            fail("MongoCommandException expected");
        } catch (MongoCommandException e) {
            assertThat(e.getCode()).isEqualTo(48);
            assertThat(e.getMessage()).contains("collection already exists");
        }
    }

    @Test
    public void testUnsupportedModifier() throws Exception {
        collection.insertOne(json("{}"));
        try {
            collection.updateOne(json("{}"), json("$foo: {}"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getCode()).isEqualTo(10147);
            assertThat(e.getMessage()).contains("Invalid modifier specified: $foo");
        }
    }

    @Test
    public void testUpsertWithInc() {
        Document query = json("_id:{ f: 'ca', '1': { l: 2 }, t: { t: 11 } }");
        Document update = json("'$inc': { 'n.!' : 1 , 'n.a.b:false' : 1}");

        collection.updateOne(query, update, new UpdateOptions().upsert(true));

        query.putAll(json("n: {'!': 1, a: {'b:false': 1}}"));
        assertThat(collection.find().first()).isEqualTo(query);
    }

    @Test
    public void testBasicUpdate() {
        collection.insertOne(json("_id:1"));
        collection.insertOne(json("_id:2, b:5"));
        collection.insertOne(json("_id:3"));
        collection.insertOne(json("_id:4"));

        collection.replaceOne(json("_id:2"), json("_id:2, a:5"));

        assertThat(collection.find(json("_id:2")).first()).isEqualTo(json("_id:2, a:5"));
    }

    @Test
    public void testCollectionStats() throws Exception {
        try {
            getCollStats();
            fail("MongoCommandException expected");
        } catch (MongoCommandException e) {
            assertThat(e.getCode()).isEqualTo(26);
            assertThat(e.getMessage()).contains("No such collection");
        }

        collection.insertOne(json("{}"));
        collection.insertOne(json("abc: 'foo'"));

        Document stats = getCollStats();
        assertThat(stats.getInteger("ok")).isEqualTo(1);
        assertThat(stats.getInteger("count").longValue()).isEqualTo(2);
        assertThat(stats.getLong("size").longValue()).isEqualTo(57);
        assertThat(stats.getDouble("avgObjSize").doubleValue()).isEqualTo(28.5);
    }

    private Document getCollStats() {
        String collectionName = collection.getNamespace().getCollectionName();
        return getCollectionStatistics(db, collectionName);
    }

    @Test
    public void testGetLogStartupWarnings() throws Exception {
        Document startupWarnings = getAdminDb().runCommand(json("getLog: 'startupWarnings'"));
        assertThat(startupWarnings.getInteger("ok")).isEqualTo(1);
        assertThat(startupWarnings.get("totalLinesWritten")).isEqualTo(0);
        assertThat(startupWarnings.get("log")).isEqualTo(Collections.emptyList());
    }

    @Test
    public void testGetLogWhichDoesNotExist() throws Exception {
        try {
            getAdminDb().runCommand(json("getLog: 'illegal'"));
            fail("MongoCommandException expected");
        } catch (MongoCommandException e) {
            assertThat(e.getMessage()).contains("no RamLog");
        }
    }

    @Test
    public void testCompoundDateIdUpserts() {
        Document query = json("{ _id : { $lt : { n: 'a' , t: 10} , $gte: { n: 'a', t: 1}}}");

        List<Document> toUpsert = Arrays.asList(
                json("_id: {n:'a', t: 1}"),
                json("_id: {n:'a', t: 2}"),
                json("_id: {n:'a', t: 3}"),
                json("_id: {n:'a', t: 11}"));

        for (Document dbo : toUpsert) {
            collection.replaceOne(dbo, new Document(dbo).append("foo", "bar"), new UpdateOptions().upsert(true));
        }
        List<Document> results = toArray(collection.find(query));
        assertThat(results).containsOnly(
                json("_id: {n:'a', t:1}, foo:'bar'"), //
                json("_id: {n:'a', t:2}, foo:'bar'"), //
                json("_id: {n:'a', t:3}, foo:'bar'"));
    }

    @Test
    public void testCompoundSort() {
        collection.insertOne(json("a:1, _id:1"));
        collection.insertOne(json("a:2, _id:5"));
        collection.insertOne(json("a:1, _id:2"));
        collection.insertOne(json("a:2, _id:4"));
        collection.insertOne(json("a:1, _id:3"));

        List<Document> documents = toArray(collection.find().sort(json("a:1, _id:-1")));
        assertThat(documents).containsExactly(json("a:1, _id:3"), json("a:1, _id:2"), json("a:1, _id:1"),
                json("a:2, _id:5"), json("a:2, _id:4"));
    }

    @Test
    public void testCountCommand() {
        assertThat(collection.count()).isZero();
    }

    @Test
    public void testCountWithQueryCommand() {
        collection.insertOne(json("n:1"));
        collection.insertOne(json("n:2"));
        collection.insertOne(json("n:2"));
        assertThat(collection.count(json("n:2"))).isEqualTo(2);
    }

    @Test
    public void testCreateIndexes() {
        collection.createIndex(new Document("n", 1));
        collection.createIndex(new Document("b", 1));
        List<Document> indexes = toArray(getCollection("system.indexes").find());
        assertThat(indexes).containsOnly(
            json("key:{_id:1}").append("ns", collection.getNamespace().getFullName()).append("name", "_id_"),
            json("key:{n:1}").append("ns", collection.getNamespace().getFullName()).append("name", "n_1"),
            json("key:{b:1}").append("ns", collection.getNamespace().getFullName()).append("name", "b_1"));
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
        assertThat(result.getInteger("ok")).isEqualTo(1);
        Document cursor = (Document) result.get("cursor");
        assertThat(cursor.keySet()).containsOnly("id", "ns", "firstBatch");
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
        assertThat(result.getInteger("ok")).isEqualTo(1);
        Document cursor = (Document) result.get("cursor");
        assertThat(cursor.keySet()).containsOnly("id", "ns", "firstBatch");
        assertThat(cursor.get("id")).isEqualTo(Long.valueOf(0));
        assertThat(cursor.get("ns")).isEqualTo(db.getName() + ".$cmd.listCollections");
        assertThat(cursor.get("firstBatch")).isInstanceOf(List.class);
        @SuppressWarnings("unchecked")
        List<Document> firstBatch = (List<Document>) cursor.get("firstBatch");

        Set<String> expectedCollections = new HashSet<>();
        expectedCollections.addAll(collections);
        expectedCollections.add("system.indexes");

        assertThat(firstBatch).hasSize(expectedCollections.size());

        Set<String> collectionNames = new HashSet<>();
        for (Document collection : firstBatch) {
            assertThat(collection.keySet()).containsOnly("name", "options");
            assertThat(collection.get("options")).isEqualTo(json("{}"));
            assertThat(collection.get("name")).isInstanceOf(String.class);
            collectionNames.add((String) collection.get("name"));
        }

        assertThat(collectionNames).isEqualTo(expectedCollections);
    }

    @Test
    public void testGetCollectionNames() throws Exception {
        getCollection("foo").insertOne(json("{}"));
        getCollection("bar").insertOne(json("{}"));

        List<String> collectionNames = toArray(db.listCollectionNames());
        assertThat(collectionNames).containsOnly("system.indexes", "foo", "bar");
    }

    @Test
    public void testSystemIndexes() throws Exception {
        getCollection("foo").insertOne(json("{}"));
        getCollection("bar").insertOne(json("{}"));

        MongoCollection<Document> systemIndexes = db.getCollection("system.indexes");
        assertThat(toArray(systemIndexes.find())).containsOnly(json("name: '_id_', ns: 'testdb.foo', key: {_id: 1}"),
                json("name: '_id_', ns: 'testdb.bar', key: {_id: 1}"));
    }

    @Test
    public void testSystemNamespaces() throws Exception {
        getCollection("foo").insertOne(json("{}"));
        getCollection("bar").insertOne(json("{}"));

        MongoCollection<Document> namespaces = db.getCollection("system.namespaces");
        assertThat(toArray(namespaces.find())).containsOnly(json("name: 'testdb.system.indexes'"),
                json("name: 'testdb.foo'"), json("name: 'testdb.bar'"));
    }

    @Test
    public void testDatabaseStats() throws Exception {
        Document stats = db.runCommand(new Document("dbStats", 1).append("scale", 1));
        assertThat(stats.getInteger("ok")).isEqualTo(1);
        assertThat(stats.getLong("objects")).isZero();
        assertThat(stats.getInteger("collections")).isZero();
        assertThat(stats.getInteger("indexes")).isZero();
        assertThat(stats.getLong("dataSize")).isZero();

        getCollection("foo").insertOne(json("{}"));
        getCollection("foo").insertOne(json("{}"));
        getCollection("bar").insertOne(json("{}"));

        stats = db.runCommand(new Document("dbStats", 1).append("scale", 1));
        assertThat(stats.getInteger("ok")).isEqualTo(1);
        assertThat(stats.getLong("objects")).isEqualTo(8);
        assertThat(stats.getInteger("collections")).isEqualTo(3);
        assertThat(stats.getInteger("indexes")).isEqualTo(2);
        assertThat(stats.getLong("dataSize")).isEqualTo(271);
    }

    @Test
    public void testDeleteDecrementsCount() {
        collection.insertOne(json("key: 'value'"));
        assertThat(collection.count()).isEqualTo(1);
        collection.deleteOne(json("{}"));
        assertThat(collection.count()).isZero();
    }

    @Test
    public void testDeleteInSystemNamespace() throws Exception {
        try {
            getCollection("system.foobar").deleteOne(json("{}"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getCode()).isEqualTo(12050);
            assertThat(e.getMessage()).contains("cannot delete from system namespace");
        }

        try {
            getCollection("system.namespaces").deleteOne(json("{}"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getCode()).isEqualTo(12050);
            assertThat(e.getMessage()).contains("cannot delete from system namespace");
        }
    }

    @Test
    public void testUpdateInSystemNamespace() throws Exception {
        for (String collectionName : Arrays.asList("system.foobar", "system.namespaces")) {
            MongoCollection<Document> collection = getCollection(collectionName);
            try {
                collection.updateMany(eq("some", "value"), set("field", "value"));
                fail("MongoException expected");
            } catch (MongoException e) {
                assertThat(e.getCode()).isEqualTo(10156);
                assertThat(e.getMessage()).contains("cannot update system collection");
            }
        }
    }

    @Test
    public void testDistinctQuery() {
        collection.insertOne(new Document("n", 3));
        collection.insertOne(new Document("n", 1));
        collection.insertOne(new Document("n", 2));
        collection.insertOne(new Document("n", 1));
        collection.insertOne(new Document("n", 1));
        assertThat(toArray(collection.distinct("n", Integer.class))).containsExactly(1, 2, 3);
        assertThat(toArray(collection.distinct("n", json("n: {$gt: 1}"), Integer.class))).containsExactly(2, 3);
        assertThat(collection.distinct("foobar", String.class)).isEmpty();
        assertThat(collection.distinct("_id", ObjectId.class)).hasSize((int) collection.count());
    }

    @Test
    public void testDropCollection() throws Exception {
        collection.insertOne(json("{}"));
        assertThat(toArray(db.listCollectionNames())).contains(collection.getNamespace().getCollectionName());
        collection.drop();
        assertThat(toArray(db.listCollectionNames())).doesNotContain(collection.getNamespace().getCollectionName());
    }

    @Test
    public void testDropCollectionAlsoDropsFromDB() throws Exception {
        collection.insertOne(json("{}"));
        collection.drop();
        assertThat(collection.count()).isZero();
        assertThat(toArray(db.listCollectionNames())).doesNotContain(collection.getNamespace().getCollectionName());
    }

    @Test
    public void testDropDatabaseAlsoDropsCollectionData() throws Exception {
        collection.insertOne(json("{}"));
        db.drop();
        assertThat(collection.count()).isZero();
    }

    @Test
    public void testDropDatabaseDropsAllData() throws Exception {
        collection.insertOne(json("{}"));
        MongoCollection<Document> collection2 = getCollection("testcoll2");
        collection2.insertOne(json("{}"));

        syncClient.dropDatabase(db.getName());
        assertThat(listDatabaseNames()).doesNotContain(db.getName());
        assertThat(collection.count()).isZero();
        assertThat(toArray(db.listCollectionNames())).doesNotContain(collection.getNamespace().getCollectionName(),
                collection2.getNamespace().getCollectionName());
    }

    @Test
    public void testEmbeddedSort() {
        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));
        collection.insertOne(json("_id: 3"));
        collection.insertOne(json("_id: 4, counts:{done:1}"));
        collection.insertOne(json("_id: 5, counts:{done:2}"));

        List<Document> objs = toArray(collection.find(ne("c", true)).sort(json("\"counts.done\": -1, _id: 1")));
        assertThat(objs).containsExactly(
                json("_id: 5, counts:{done:2}"),
                json("_id: 4, counts:{done:1}"),
                json("_id: 1"),
                json("_id: 2"),
                json("_id: 3"));
    }

    @Test
    public void testFindAndModifyCommandEmpty() throws Exception {
        Document cmd = new Document("findandmodify", collection.getNamespace().getCollectionName());
        try {
            db.runCommand(cmd);
            fail("MongoCommandException expected");
        } catch (MongoCommandException e) {
            assertThat(e.getMessage()).contains("need remove or update");
        }
    }

    @Test
    public void testFindAndModifyCommandIllegalOp() throws Exception {
        collection.insertOne(json("_id: 1"));

        Document cmd = new Document("findAndModify", collection.getNamespace().getCollectionName());
        cmd.put("query", json("_id: 1"));
        cmd.put("update", new Document("$inc", json("_id: 1")));

        assertThat(collection.find().first()).isEqualTo(json("_id: 1"));

        try {
            db.runCommand(cmd);
            fail("MongoCommandException expected");
        } catch (MongoCommandException e) {
            assertThat(e.getCode()).isEqualTo(10148);
            assertThat(e.getMessage()).contains("Mod on _id not allowed");
        }
    }

    @Test
    public void testFindAndModifyCommandUpdate() throws Exception {
        collection.insertOne(json("_id: 1"));

        Document cmd = new Document("findAndModify", collection.getNamespace().getCollectionName());
        cmd.put("query", json("_id: 1"));
        cmd.put("update", json("$inc: {a: 1}"));

        Document result = db.runCommand(cmd);
        assertThat(result.get("lastErrorObject")).isEqualTo(json("updatedExisting: true, n: 1"));
        assertThat(result.getInteger("ok")).isEqualTo(1);

        assertThat(collection.find().first()).isEqualTo(json("_id: 1, a: 1"));
    }

    @Test
    public void testFindOneAndUpdateError() throws Exception {
        collection.insertOne(json("_id: 1, a: 1"));

        try {
            collection.findOneAndUpdate(json("_id: 1"), json("$inc: {_id: 1}"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getCode()).isEqualTo(10148);
            assertThat(e.getMessage()).contains("Mod on _id not allowed");
        }
    }

    @Test
    public void testFindOneAndUpdateFields() throws Exception {
        collection.insertOne(json("_id: 1, a: 1"));
        Document result = collection.findOneAndUpdate(json("_id: 1"), json("$inc: {a:1}"),
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));

        assertThat(result).isEqualTo(json("_id: 1, a: 2"));
    }

    @Test
    public void testFineOneAndUpdateNotFound() throws Exception {
        collection.insertOne(json("_id: 1, a: 1"));
        Document result = collection.findOneAndUpdate(json("_id: 2"), new Document("$inc", json("a: 1")));

        assertThat(result).isNull();
        assertThat(collection.count()).isEqualTo(1);
    }

    @Test
    public void testFineOneAndUpdateRemove() {
        collection.insertOne(json("_id: 1, a: 1"));
        Document result = collection.findOneAndDelete(json("_id: 1"));

        assertThat(result).isEqualTo(json("_id: 1, a: 1"));
        assertThat(collection.count()).isZero();
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
        collection.insertOne(json("_id: 1, a:15"));
        collection.insertOne(json("_id: 2, a:10"));
        collection.insertOne(json("_id: 3, a:20"));

        Document order = json("a:1");
        Document result = collection.findOneAndUpdate(json("{}"), json("$inc: {a: 1}"),
            new FindOneAndUpdateOptions().sort(order).returnDocument(ReturnDocument.AFTER));
        assertThat(result).isEqualTo(json("_id: 2, a: 11"));

        order = json("a: -1");
        result = collection.findOneAndUpdate(json("{}"), json("$inc: {a: 1}"),
            new FindOneAndUpdateOptions().sort(order).returnDocument(ReturnDocument.AFTER));
        assertThat(result).isEqualTo(json("_id: 3, a: 21"));
    }

    @Test
    public void testFindOneAndUpdateUpsert() {
        Document result = collection.findOneAndUpdate(json("_id: 1"), json("$inc: {a:1}"),
            new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));

        assertThat(result).isEqualTo(json("_id: 1, a: 1"));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, a: 1"));
    }

    @Test
    public void testFindOneAndUpdateUpsertReturnBefore() {
        Document result = collection.findOneAndUpdate(json("_id: 1"), json("$inc: {a:1}"),
            new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.BEFORE));

        assertThat(result).isEqualTo(json("{}"));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, a: 1"));
    }

    @Test
    public void testFindAndRemoveFromEmbeddedList() {
        collection.insertOne(json("_id: 1, a: [1]"));
        Document result = collection.findOneAndDelete(json("_id: 1"));
        assertThat(result).isEqualTo(json("_id: 1, a: [1]"));
        assertThat(collection.count()).isZero();
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
        Document result = collection.find(json("_id: {$in: [1,2]}")).first();
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
        assertThat(actual).containsOnly(json("_id: 1"), json("_id: 2"));
    }

    @Test
    public void testFindWithPattern() {
        collection.insertOne(json("_id: 'marta'"));
        collection.insertOne(json("_id: 'john', foo: 'bar'"));
        collection.insertOne(json("_id: 'jon', foo: 'ba'"));
        collection.insertOne(json("_id: 'jo'"));

        assertThat(toArray(collection.find(new Document("_id", Pattern.compile("mart")))))
            .containsOnly(json("_id: 'marta'"));

        assertThat(toArray(collection.find(new Document("foo", Pattern.compile("ba")))))
            .containsOnly(json("_id: 'john', foo: 'bar'"), json("_id: 'jon', foo: 'ba'"));

        assertThat(toArray(collection.find(new Document("foo", Pattern.compile("ba$")))))
            .containsOnly(json("_id: 'jon', foo: 'ba'"));
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

        assertThat(toArray(collection.find().sort(json("_id: 1")).limit(2).skip(2))).containsExactly(json("_id: 3"),
            json("_id: 4"));
    }

    @Test
    public void testFindWithSkipLimitInReverseOrder() {
        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));
        collection.insertOne(json("_id: 3"));
        collection.insertOne(json("_id: 4"));

        assertThat(toArray(collection.find().sort(json("_id: -1")).limit(2).skip(2))).containsExactly(json("_id: 2"),
            json("_id: 1"));
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

        assertThat(toArray(collection.find().sort(json("_id: 1")).limit(2).skip(2))).containsExactly(json("_id: 5"));
    }

    @Test
    public void testFullUpdateWithSameId() throws Exception {
        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2, b: 5"));
        collection.insertOne(json("_id: 3"));
        collection.insertOne(json("_id: 4"));

        collection.replaceOne(json("_id: 2, b:5"), json("_id: 2, a:5"));

        assertThat(collection.find(json("_id: 2")).first()).isEqualTo(json("_id: 2, a:5"));
    }

    @Test
    public void testGetCollection() {
        MongoCollection<Document> collection = getCollection("coll");
        getCollection("coll").insertOne(json("{}"));

        assertThat(collection).isNotNull();
        assertThat(toArray(db.listCollectionNames())).contains("coll");
    }

    @Test
    public void testNullId() throws Exception {
        collection.insertOne(json("{_id: null, name: 'test'}"));
        Document result = collection.find(json("name: 'test'")).first();
        assertThat(result).isNotNull();
        assertThat(result.getObjectId(Constants.ID_FIELD)).isNull();

        try {
            collection.insertOne(json("_id: null"));
            fail("MongoWriteException expected");
        } catch (MongoWriteException e) {
            assertThat(e.getMessage()).contains("duplicate key error");
        }

        assertThat(collection.count()).isEqualTo(1);
        assertThat(collection.find(json("_id: null")).first()).isEqualTo(json("{_id: null, name: 'test'}"));

        collection.deleteOne(json("_id: null"));
        assertThat(collection.count()).isZero();
    }

    @Test
    public void testIdInQueryResultsInIndexOrder() {
        collection.insertOne(json("_id: 4"));
        collection.insertOne(json("_id: 3"));
        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));

        List<Document> docs = toArray(collection.find(json("_id: {$in: [3,2,1]}")));
        assertThat(docs).containsExactlyInAnyOrder(json("_id: 1"), json("_id: 2"), json("_id: 3"));
    }

    @Test
    public void testIdNotAllowedToBeUpdated() {
        collection.insertOne(json("_id: 1"));

        try {
            collection.replaceOne(json("_id: 1"), json("_id:2, a:4"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getMessage()).contains("cannot change _id of a document old: 1, new: 2");
        }

        // test with $set

        try {
            collection.updateOne(json("_id: 1"), new Document("$set", json("_id: 2")));
            fail("should throw exception");
        } catch (MongoException e) {
            assertThat(e.getMessage()).contains("Mod on _id not allowed");
        }
    }

    @Test
    public void testIllegalCommand() throws Exception {
        try {
            db.runCommand(json("foo: 1"));
            fail("MongoCommandException expected");
        } catch (MongoCommandException e) {
            assertThat(e.getMessage()).contains("no such cmd: foo");
        }
    }

    @Test
    public void testInsert() throws Exception {
        assertThat(collection.count()).isEqualTo(0);

        for (int i = 0; i < 3; i++) {
            collection.insertOne(new Document("_id", Integer.valueOf(i)));
        }

        assertThat(collection.count()).isEqualTo(3);

        collection.insertOne(json("foo: [1,2,3]"));

        collection.insertOne(new Document("foo", new byte[10]));
        Document insertedObject = new Document("foo", UUID.randomUUID());
        collection.insertOne(insertedObject);
        Document document = collection.find(insertedObject).first();
        assertThat(document).isEqualTo(insertedObject);
    }

    @Test
    public void testInsertDuplicate() throws Exception {
        assertThat(collection.count()).isEqualTo(0);

        collection.insertOne(json("_id: 1"));
        assertThat(collection.count()).isEqualTo(1);

        try {
            collection.insertOne(json("_id: 1"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getMessage()).contains("duplicate key error");
        }

        assertThat(collection.count()).isEqualTo(1);
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
        assertThat(collection.count()).isZero();
        collection.insertOne(json("key: 'value'"));
        assertThat(collection.count()).isEqualTo(1);
    }

    @Test
    public void testInsertQuery() throws Exception {
        assertThat(collection.count()).isEqualTo(0);

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
            assertThat(collection.count()).isEqualTo(1);
            collection.deleteOne(json("_id: 1"));
            assertThat(collection.count()).isZero();

            collection.insertOne(new Document("_id", i));
            collection.deleteOne(new Document("_id", i));
        }
        assertThat(collection.count()).isZero();
        collection.deleteOne(json("'doesnt exist': 1"));
        assertThat(collection.count()).isZero();
    }

    @Test
    public void testInsertInSystemNamespace() throws Exception {
        try {
            getCollection("system.foobar").insertOne(json("{}"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getCode()).isEqualTo(16459);
            assertThat(e.getMessage()).contains("attempt to insert in system namespace");
        }

        try {
            getCollection("system.namespaces").insertOne(json("{}"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getCode()).isEqualTo(16459);
            assertThat(e.getMessage()).contains("attempt to insert in system namespace");
        }
    }

    @Test
    public void testListDatabaseNames() throws Exception {
        assertThat(listDatabaseNames()).isEmpty();
        collection.insertOne(json("{}"));
        assertThat(listDatabaseNames()).containsExactly(db.getName());
        getDatabase().getCollection("some-collection").insertOne(json("{}"));
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
        assertThat(collection.count()).isEqualTo(0);
    }

    @Test
    public void testQueryAll() throws Exception {
        List<Object> inserted = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Document obj = new Document("_id", i);
            collection.insertOne(obj);
            inserted.add(obj);
        }
        assertThat(collection.count()).isEqualTo(10);

        assertThat(toArray(collection.find().sort(json("_id: 1")))).isEqualTo(inserted);
    }

    @Test
    public void testQueryCount() throws Exception {
        for (int i = 0; i < 100; i++) {
            collection.insertOne(json("{}"));
        }
        assertThat(collection.count()).isEqualTo(100);

        Document obj = json("_id: 1");
        assertThat(collection.count(obj)).isEqualTo(0);
        collection.insertOne(obj);
        assertThat(collection.count(obj)).isEqualTo(1);
    }

    @Test
    public void testQueryLimitEmptyQuery() throws Exception {
        for (int i = 0; i < 5; i++) {
            collection.insertOne(json("{}"));
        }
        assertThat(collection.count(json("{}"), new CountOptions().limit(1))).isEqualTo(1);
        assertThat(collection.count(json("{}"), new CountOptions().limit(-1))).isEqualTo(5);
        assertThat(collection.count(json("{}"))).isEqualTo(5);
    }

    @Test
    public void testQueryLimitSimpleQuery() throws Exception {
        for (int i = 0; i < 5; i++) {
            collection.insertOne(json("a:1"));
        }
        assertThat(collection.count(json("a:1"), new CountOptions().limit(1))).isEqualTo(1);
        assertThat(collection.count(json("a:1"), new CountOptions().limit(-1))).isEqualTo(5);
        assertThat(collection.count(json("a:1"))).isEqualTo(5);
    }

    @Test
    public void testQueryNull() throws Exception {
        Document object = json("_id: 1");
        collection.insertOne(object);
        assertThat(collection.find(json("foo: null")).first()).isEqualTo(object);
    }

    @Test
    public void testQuerySkipLimitEmptyQuery() throws Exception {
        assertThat(collection.count(json("{}"), new CountOptions().skip(3))).isEqualTo(0);

        for (int i = 0; i < 10; i++) {
            collection.insertOne(json("{}"));
        }

        assertThat(collection.count(json("{}"), new CountOptions().skip(3))).isEqualTo(7);
        assertThat(collection.count(json("{}"), new CountOptions().skip(15))).isEqualTo(0);
        assertThat(collection.count(json("{}"), new CountOptions().skip(3).limit(5))).isEqualTo(5);
    }

    @Test
    public void testQuerySkipLimitSimpleQuery() throws Exception {
        assertThat(collection.count(json("a:1"), new CountOptions().skip(3))).isEqualTo(0);

        for (int i = 0; i < 10; i++) {
            collection.insertOne(json("a:1"));
        }

        assertThat(collection.count(json("a:1"), new CountOptions().skip(3))).isEqualTo(7);
        assertThat(collection.count(json("a:1"), new CountOptions().skip(3).limit(5))).isEqualTo(5);
        assertThat(collection.count(json("a:1"), new CountOptions().skip(15).limit(5))).isEqualTo(0);
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
        Document obj = collection.find(json("{}")).projection(json("foo: 1")).first();
        assertThat(obj.keySet()).containsOnly("_id", "foo");

        obj = collection.find(json("foo:'bar'")).projection(json("_id: 1")).first();
        assertThat(obj.keySet()).containsOnly("_id");

        obj = collection.find(json("foo: 'bar'")).projection(json("_id: 0, foo:1")).first();
        assertThat(obj.keySet()).containsOnly("foo");
    }

    @Test
    public void testQueryWithDotNotationFieldSelector() throws Exception {
        collection.insertOne(json("_id: 123, index: false, foo: { a: 'a1', b: 0}"));
        Document obj = collection.find(json("{}")).projection(json("'foo.a': 1, 'foo.b': 1")).first();
        assertThat(obj).isEqualTo(json("_id: 123, foo: {a: 'a1', b: 0}"));

        obj = collection.find(json("{}")).projection(json("'foo.a': 1")).first();
        assertThat(obj).isEqualTo(json("_id: 123, foo: {a: 'a1'}"));

        obj = collection.find(json("{}")).projection(json("'foo.a': 1, index: 1, _id: 0")).first();
        assertThat(obj).isEqualTo(json("foo: {a: 'a1'}, index: false"));

        obj = collection.find(json("{}")).projection(json("foo: 1, _id: 0")).first();
        assertThat(obj).isEqualTo(json("foo: {a: 'a1', b: 0}"));

        obj = collection.find(json("{}")).projection(json("'foo.a.b.c.d': 1")).first();
        assertThat(obj).isEqualTo(json("_id: 123, foo: {}"));
    }

    @Test
    public void testQuerySystemNamespace() throws Exception {
        assertThat(getCollection("system.foobar").find().first()).isNull();
        assertThat(db.listCollectionNames()).isEmpty();

        collection.insertOne(json("{}"));
        Document expectedObj = new Document("name", collection.getNamespace().getFullName());
        Document coll = getCollection("system.namespaces").find(expectedObj).first();
        assertThat(coll).isEqualTo(expectedObj);
    }

    @Test
    public void testQueryAllExpression() throws Exception {
        collection.insertOne(json(" _id : [ { x : 1 } , { x : 2  } ]"));
        collection.insertOne(json(" _id : [ { x : 2 } , { x : 3  } ]"));

        assertThat(collection.count(json("'_id.x':{$all:[1,2]}"))).isEqualTo(1);
        assertThat(collection.count(json("'_id.x':{$all:[2,3]}"))).isEqualTo(1);
    }

    @Test
    public void testQueryWithSubdocumentIndex() throws Exception {
        collection.createIndex(json("action:{actionId:1}"), new IndexOptions().unique(true));

        collection.insertOne(json("action: { actionId: 1 }, value: 'a'"));
        collection.insertOne(json("action: { actionId: 2 }, value: 'b'"));
        collection.insertOne(json("action: { actionId: 3 }, value: 'c'"));

        Document foundWithNestedDocument = collection.find(json("action: { actionId: 2 }")).first();
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
        assertThat(collection.count()).isEqualTo(3);

        collection.deleteMany(json("_id: {$gte: 3}"));
        assertThat(collection.count()).isEqualTo(1);
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
        collection.insertOne(json("{}"));
        collection.insertOne(json("{}"));

        DeleteResult result = collection.deleteMany(json("{}"));
        assertThat(result.getDeletedCount()).isEqualTo(2);

        result = collection.deleteMany(json("{}"));
        assertThat(result.getDeletedCount()).isEqualTo(0);
    }

    @Test
    public void testReservedCollectionNames() throws Exception {
        try {
            getCollection("foo$bar").insertOne(json("{}"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getMessage()).contains("cannot insert into reserved $ collection");
        }

        String veryLongString = "verylongstring";
        for (int i = 0; i < 5; i++) {
            veryLongString += veryLongString;
        }
        try {
            getCollection(veryLongString).insertOne(json("{}"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getMessage()).contains("name too long");
        }
    }

    @Test
    public void testServerStatus() throws Exception {
        Date before = new Date();
        Document serverStatus = runCommand("serverStatus");
        assertThat(serverStatus.getInteger("ok")).isEqualTo(1);
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
        Document response = runCommand("ping");
        assertThat(response.getInteger("ok")).isEqualTo(1);
    }

    @Test
    public void testPingTrue() throws Exception {
        Document command = new Document("ping", Boolean.TRUE);
        Document response = runCommand(command);
        assertThat(response.getInteger("ok")).isEqualTo(1);
    }

    @Test
    public void testReplSetGetStatus() throws Exception {
        try {
            runCommand("replSetGetStatus");
            fail("MongoCommandException expected");
        } catch (MongoCommandException e) {
            assertThat(e.getErrorMessage()).contains("not running with --replSet");
        }
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
    public void testSort() {
        collection.insertOne(json("a:1, _id:1"));
        collection.insertOne(json("a:2, _id:2"));
        collection.insertOne(json("_id: 5"));
        collection.insertOne(json("a:3, _id:3"));
        collection.insertOne(json("a:4, _id:4"));

        List<Document> objs = toArray(collection.find().sort(json("a: -1")));
        assertThat(objs).containsExactly(
            json("a:4, _id:4"),
            json("a:3, _id:3"),
            json("a:2, _id:2"),
            json("a:1, _id:1"),
            json("_id: 5")
        );
    }

    @Test
    public void testSortByEmbeddedKey() {
        collection.insertOne(json("_id: 1, a: { b:1 }"));
        collection.insertOne(json("_id: 2, a: { b:2 }"));
        collection.insertOne(json("_id: 3, a: { b:3 }"));
        List<Document> results = toArray(collection.find().sort(json("'a.b': -1")));
        assertThat(results).containsExactly(
            json("_id: 3, a: { b:3 }"),
            json("_id: 2, a: { b:2 }"),
            json("_id: 1, a: { b:1 }")
        );
    }

    @Test
    public void testUpdate() throws Exception {
        Document object = json("_id: 1");
        Document newObject = json("{_id: 1, foo: 'bar'}");

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

        collection.updateOne(json("{}"), json("$set: {c:5}"));
        assertThat(collection.find().first()).isEqualTo(json("'': 1, _id: 2, a: 3, b: 4, c:5"));
    }

    @Test
    public void testUpdateEmptyPositional() throws Exception {
        collection.insertOne(json("{}"));
        try {
            collection.updateOne(json("{}"), json("$set:{'a.$.b': 1}"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getCode()).isEqualTo(16650);
            assertThat(e.getMessage()).contains("Cannot apply the positional operator without a corresponding query field containing an array.");
        }
    }

    @Test
    public void testUpdateMultiplePositional() throws Exception {
        collection.insertOne(json("{a: {b: {c: 1}}}"));
        try {
            collection.updateOne(json("{'a.b.c':1}"), json("$set:{'a.$.b.$.c': 1}"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getCode()).isEqualTo(16650);
            assertThat(e.getMessage()).contains("Cannot apply the positional operator without a corresponding query field containing an array.");
        }
    }

    @Test
    public void testUpdateIllegalFieldName() throws Exception {

        // Disallow $ in field names - SERVER-3730

        collection.insertOne(json("{x:1}"));

        collection.updateOne(json("{x:1}"), json("$set: {y:1}")); // ok

        try {
            collection.updateOne(json("{x:1}"), json("$set: {$z:1}"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getCode()).isEqualTo(15896);
            assertThat(e.getMessage()).contains("Modified field name may not start with $");
        }

        // unset ok to remove bad fields
        collection.updateOne(json("{x:1}"), json("$unset: {$z:1}"));

        try {
            collection.updateOne(json("{x:1}"), json("$inc: {$z:1}"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getCode()).isEqualTo(15896);
            assertThat(e.getMessage()).contains("Modified field name may not start with $");
        }

        try {
            collection.updateOne(json("{x:1}"), json("$pushAll: {$z:[1,2,3]}"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getCode()).isEqualTo(15896);
            assertThat(e.getMessage()).contains("Modified field name may not start with $");
        }

    }

    @Test
    public void testUpdateSubdocument() throws Exception {
        try {
            collection.updateOne(json("{}"), json("'a.b.c': 123"));
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).contains("Invalid BSON field name a.b.c");
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
        Document expected = json("_id: 1, field:{subfield:{subsubfield: ['value']}}");
        assertThat(collection.find(idObj).first()).isEqualTo(expected);

        // push to non-array
        collection.updateOne(idObj, json("$set: {field: 'value'}"));
        try {
            collection.updateOne(idObj, json("$push: {field: 'value'}"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getCode()).isEqualTo(10141);
            assertThat(e.getMessage()).contains("Cannot apply $push modifier to non-array");
        }

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
        try {
            collection.updateOne(idObj, json("$pushAll: {field: 'value'}"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getCode()).isEqualTo(10153);
            assertThat(e.getMessage()).contains("Modifier $pushAll allowed for arrays only");
        }

        collection.updateOne(idObj, json("$pushAll: {field: ['value', 'value2']}"));
        assertThat(collection.find(idObj).first()).isEqualTo(json("_id: 1, field: ['value', 'value2']"));
    }

    @Test
    public void testUpdateAddToSet() throws Exception {
        Document idObj = json("_id: 1");
        collection.insertOne(idObj);
        collection.updateOne(idObj, json("$addToSet: {'field.subfield.subsubfield': 'value'}"));
        assertThat(collection.find(idObj).first()).isEqualTo(json("_id: 1, field:{subfield:{subsubfield:['value']}}"));

        // addToSet to non-array
        collection.updateOne(idObj, json("$set: {field: 'value'}"));
        try {
            collection.updateOne(idObj, json("$addToSet: {field: 'value'}"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getCode()).isEqualTo(10141);
            assertThat(e.getMessage()).contains("Cannot apply $addToSet modifier to non-array");
        }

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
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, a: [6,5,4]"));

        collection.updateOne(json("_id: 1"), addEachToSet("a", Arrays.asList(3, 2, 1)));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, a: [6,5,4,3,2,1]"));

        collection.updateOne(json("_id: 1"), addEachToSet("a", Arrays.asList(7, 7, 9, 2)));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, a: [6,5,4,3,2,1,7,9]"));

        collection.updateOne(json("_id: 1"), addEachToSet("a", Arrays.asList(12, 13, 12)));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, a: [6,5,4,3,2,1,7,9,12,13]"));
    }

    @Test
    public void testUpdateDatasize() throws Exception {
        Document obj = json("{_id:1, a:{x:[1, 2, 3]}}");
        collection.insertOne(obj);
        Number oldSize = getCollStats().getLong("size");

        collection.updateOne(json("_id:1"), set("a.x.0", 3));
        assertThat(collection.find().first().get("a")).isEqualTo(json("x:[3,2,3]"));
        Number newSize = getCollStats().getLong("size");
        assertThat(newSize).isEqualTo(oldSize);

        // now increase the db
        collection.updateOne(json("_id:1"), set("a.x.0", "abc"));
        Number yetNewSize = getCollStats().getLong("size");
        assertThat(yetNewSize.longValue() - oldSize.longValue()).isEqualTo(4);
    }

    @Test
    public void testUpdatePull() throws Exception {
        Document obj = json("_id: 1");
        collection.insertOne(obj);

        // pull from non-existing field
        assertThat(collection.find(obj).first()).isEqualTo(obj);

        // pull from non-array
        collection.updateOne(obj, set("field", "value"));
        try {
            collection.updateOne(obj, pull("field", "value"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getCode()).isEqualTo(10142);
            assertThat(e.getMessage()).contains("Cannot apply $pull modifier to non-array");
        }

        // pull standard
        collection.updateOne(obj, json("$set: {field: ['value1', 'value2', 'value1']}"));

        collection.updateOne(obj, pull("field", "value1"));

        assertThat(collection.find(obj).first().get("field")).isEqualTo(Collections.singletonList("value2"));

        // pull with multiple fields

        collection.updateOne(obj, json("{$set: {field1: ['value1', 'value2', 'value1']}}"));
        collection.updateOne(obj, json("$set: {field2: ['value3', 'value3', 'value1']}"));

        collection.updateOne(obj, json("$pull: {field1: 'value2', field2: 'value3'}"));

        assertThat(collection.find(obj).first().get("field1")).isEqualTo(Arrays.asList("value1", "value1"));
        assertThat(collection.find(obj).first().get("field2")).isEqualTo(Collections.singletonList("value1"));
    }

    @Test
    public void testUpdatePullValueWithCondition() {
        collection.insertOne(json("_id: 1, votes: [ 3, 5, 6, 7, 7, 8 ]"));
        collection.updateOne(json("_id: 1"), json("$pull: { votes: { $gte: 6 } }"));

        assertThat(collection.find().first()).isEqualTo(json("_id: 1, votes: [ 3, 5 ]"));
    }

    @Test
    public void testUpdatePullDocuments() {
        collection.insertOne(json("_id: 1, results: [{item: 'A', score: 5}, {item: 'B', score: 8, comment: 'foobar'}]"));
        collection.insertOne(json("_id: 2, results: [{item: 'C', score: 8, comment: 'foobar'}, {item: 'B', score: 4}]"));

        collection.updateOne(json("{}"), json("$pull: { results: { score: 8 , item: 'B' } }"));

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
        try {
            collection.updateOne(obj, json("$pullAll: {field: 'value'}"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getCode()).isEqualTo(10142);
            assertThat(e.getMessage()).contains("Cannot apply $pullAll modifier to non-array");
        }

        collection.updateOne(obj, json("$set: {field1: ['value1', 'value2', 'value1', 'value3', 'value4', 'value3']}"));

        collection.updateOne(obj, json("$pullAll: {field1: ['value1', 'value3']}"));

        assertThat(collection.find(obj).first().get("field1")).isEqualTo(Arrays.asList("value2", "value4"));

        try {
            collection.updateOne(obj, json("$pullAll: {field1: 'bar'}"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getCode()).isEqualTo(10153);
            assertThat(e.getMessage()).contains("Modifier $pullAll allowed for arrays only");
        }

    }

    @Test
    public void testUpdateSet() throws Exception {
        Document object = json("_id: 1");

        collection.insertOne(object);
        assertThat(collection.find(object).first()).isEqualTo(object);

        collection.updateOne(object, json("$set: {foo: 'bar'}"));

        Document expected = json("{}");
        expected.putAll(object);
        expected.put("foo", "bar");

        collection.updateOne(object, json("$set: {bar: 'bla'}"));
        expected.put("bar", "bla");
        assertThat(collection.find(object).first()).isEqualTo(expected);

        collection.updateOne(object, json("$set: {'foo.bar': 'bla'}"));
        expected.put("foo", json("bar: 'bla'"));
        assertThat(collection.find(object).first()).isEqualTo(expected);

        collection.updateOne(object, json("$set: {'foo.foo': '123'}"));
        ((Document) expected.get("foo")).put("foo", "123");
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

        collection.insertOne(json("_id: 1, a: [{x:0}]"));
        collection.updateOne(json("{}"), json("$set: {'a.0.x': 3}"));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, a: [{x:3}]"));

        collection.updateOne(json("{}"), json("$set: {'a.1.z': 17}"));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, a: [{x:3}, {z:17}]"));

        collection.updateOne(json("{}"), json("$set: {'a.0.y': 7}"));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, a: [{x:3, y:7}, {z:17}]"));

        collection.updateOne(json("{}"), json("$set: {'a.1': 'test'}"));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, a: [{x:3, y:7}, 'test']"));
    }

    @Test
    public void testUpdateUnsetWithArrayIndices() throws Exception {

        // SERVER-273

        collection.insertOne(json("_id: 1, a:[{x:0}]"));
        collection.updateOne(json("{}"), json("$unset: {'a.0.x': 1}"));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, a:[{}]"));

        collection.updateOne(json("{}"), json("$unset: {'a.0': 1}"));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, a:[null]"));

        collection.updateOne(json("{}"), json("$unset: {'a.10': 1}"));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, a:[null]"));
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

    // see http://docs.mongodb.org/manual/reference/operator/update/max
    @Test
    public void testUpdateMaxCompareNumbers() throws Exception {
        Document object = json("_id: 1, highScore: 800, lowScore: 200");

        collection.insertOne(object);

        collection.updateOne(json("_id: 1"), json("$max: { highScore: 950 }"));
        assertThat(collection.find(json("_id: 1")).first()).isEqualTo(json("_id: 1, highScore: 950, lowScore: 200"));

        collection.updateOne(json("_id: 1"), json("$max: { highScore: 870 }"));
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

        collection.updateOne(json("_id: 1"), json("$min: { lowScore: 150 }"));
        assertThat(collection.find(json("_id: 1")).first()).isEqualTo(json("_id: 1, highScore: 800, lowScore: 150"));

        collection.updateOne(json("_id: 1"), json("$min: { lowScore: 250 }"));
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
        collection.updateOne(object, json("$set: {'foo.bar': [1,2,3]}"));
        assertThat(collection.find(object).first()).isEqualTo(json("_id:1, foo:{bar:[1,2,3]}"));

        collection.updateOne(object, json("$pop: {'foo.bar': 1}"));
        assertThat(collection.find(object).first()).isEqualTo(json("_id:1, foo:{bar:[1,2]}"));

        collection.updateOne(object, json("$pop: {'foo.bar': -1}"));
        assertThat(collection.find(object).first()).isEqualTo(json("_id:1, foo:{bar:[2]}"));

        collection.updateOne(object, json("$pop: {'foo.bar': null}"));
        assertThat(collection.find(object).first()).isEqualTo(json("_id:1, foo:{bar:[]}"));

    }

    @Test
    public void testUpdateUnset() throws Exception {
        Document obj = json("_id: 1, a: 1, b: null, c: 'value'");
        collection.insertOne(obj);
        try {
            collection.updateOne(obj, json("$unset: {_id: ''}"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getCode()).isEqualTo(10148);
            assertThat(e.getMessage()).contains("Mod on _id not allowed");
        }

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
        Document update = json("$push: {n: {_id: 2, u:3}}, $inc: {c:4}");
        Document expected = json("_id: 1, n: [{_id: 2, u:3}], c:4");
        collection.updateOne(json("_id: {$in: [1]}"), update);
        assertThat(collection.find().first()).isEqualTo(expected);
    }

    @Test
    public void testUpdateMulti() throws Exception {
        collection.insertOne(json("a: 1"));
        collection.insertOne(json("a: 1"));
        UpdateResult result = collection.updateOne(json("a: 1"), json("$set: {b: 2}"));

        assertThat(result.getModifiedCount()).isEqualTo(1);

        assertThat(collection.count(new Document("b", 2))).isEqualTo(1);

        result = collection.updateMany(json("a: 1"), json("$set: {b: 3}"));
        assertThat(result.getModifiedCount()).isEqualTo(2);
        assertThat(collection.count(new Document("b", 2))).isEqualTo(0);
        assertThat(collection.count(new Document("b", 3))).isEqualTo(2);
    }

    @Test
    public void testUpdateIllegalInt() throws Exception {
        collection.insertOne(json("_id: 1, a: {x:1}"));

        try {
            collection.updateOne(json("_id: 1"), json("$inc: {a: 1}"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getMessage()).contains("cannot increment value");
        }

        try {
            collection.updateOne(json("_id: 1"), json("$inc: {'a.x': 'b'}"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getMessage()).contains("cannot increment with non-numeric value");
        }
    }

    @Test
    public void testUpdateWithIdInMulti() {
        collection.insertMany(Arrays.asList(json("_id: 1"), json("_id: 2")));
        collection.updateMany(json("_id: {$in:[1,2]}"), json("$set: {n:1}"));
        List<Document> results = toArray(collection.find());
        assertThat(results).containsOnly(json("_id: 1, n:1"), json("_id: 2, n: 1"));
    }

    @Test
    public void testUpdateWithIdInMultiReturnModifiedDocumentCount() {
        collection.insertMany(Arrays.asList(json("_id: 1"), json("_id: 2")));
        UpdateResult result = collection.updateMany(json("_id: {$in:[1,2]}"), json("$set:{n:1}"));
        assertThat(result.getModifiedCount()).isEqualTo(2);
    }

    @Test
    public void testUpdateWithIdQuery() {
        collection.insertMany(Arrays.asList(json("_id: 1"), json("_id: 2")));
        collection.updateMany(json("_id: {$gt:1}"), json("$set: {n:1}"));
        List<Document> results = toArray(collection.find());
        assertThat(results).containsOnly(json("_id: 1"), json("_id: 2, n:1"));
    }

    @Test
    public void testUpdateWithObjectId() {
        collection.insertOne(json("_id: {n:1}"));
        UpdateResult result = collection.updateOne(json("_id: {n:1}"), json("$set: {a:1}"));
        assertThat(result.getModifiedCount()).isEqualTo(1);
        assertThat(collection.find().first()).isEqualTo(json("_id: {n:1}, a:1"));
    }

    @Test
    public void testUpdateArrayMatch() throws Exception {

        collection.insertOne(json("_id:1, a:[{x:1,y:1}, {x:2,y:2}, {x:3,y:3}]"));

        collection.updateOne(json("'a.x': 2"), json("$inc: {'a.$.y': 1}"));

        assertThat(collection.find(json("'a.x': 2")).first()).isEqualTo(json("_id:1, a:[{x:1,y:1}, {x:2,y:3}, {x:3,y:3}]"));

        collection.insertOne(json("{'array': [{'123a':{'name': 'old'}}]}"));
        assertThat(collection.find(json("{'array.123a.name': 'old'}")).first()).isNotNull();
        collection.updateOne(json("{'array.123a.name': 'old'}"), json("{$set: {'array.$.123a.name': 'new'}}"));
        assertThat(collection.find(json("{'array.123a.name': 'new'}")).first()).isNotNull();
        assertThat(collection.find(json("{'array.123a.name': 'old'}")).first()).isNull();
    }

    @Test
    public void testMultiUpdateArrayMatch() throws Exception {
        collection.insertOne(json("{}"));
        collection.insertOne(json("x:[1,2,3]"));
        collection.insertOne(json("x:99"));

        collection.updateMany(json("x:2"), json("$inc:{'x.$': 1}"));
        assertThat(collection.find(json("x:1")).first().get("x")).isEqualTo(Arrays.asList(1, 3, 3));
    }

    @Test
    public void testUpsert() {
        UpdateResult result = collection.updateMany(json("n:'jon'"), json("$inc:{a:1}"), new UpdateOptions().upsert(true));
        assertThat(result.getModifiedCount()).isEqualTo(0);

        Document object = collection.find().first();
        assertThat(result.getUpsertedId()).isEqualTo(new BsonObjectId(object.getObjectId("_id")));

        object.remove("_id");
        assertThat(object).isEqualTo(json("n:'jon', a:1"));

        result = collection.updateOne(json("_id: 17, n:'jon'"), json("$inc:{a:1}"), new UpdateOptions().upsert(true));
        assertThat(result.getUpsertedId()).isNull();
        assertThat(collection.find(json("_id:17")).first()).isEqualTo(json("_id: 17, n:'jon', a:1"));
    }

    @Test
    public void testUpsertFieldOrder() throws Exception {
        collection.updateOne(json("'x.y': 2"), json("$inc: {a:7}"), new UpdateOptions().upsert(true));
        Document obj = collection.find().first();
        obj.remove("_id");
        // this actually differs from the official MongoDB implementation
        assertThat(obj).isEqualTo(json("x:{y:2}, a:7"));
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
        Document update1 = json("$push: {c: {a:1, b:2} }");
        Document update2 = json("$push: {c: {a:3, b:4} }");

        collection.updateOne(json("_id: 1"), update1, new UpdateOptions().upsert(true));

        collection.updateOne(json("_id: 1"), update2, new UpdateOptions().upsert(true));

        Document expected = json("_id: 1, c: [{a:1, b:2}, {a:3, b:4}]");

        assertThat(collection.find(json("'c.a':3, 'c.b':4")).first()).isEqualTo(expected);
    }

    @Test
    public void testUpsertWithConditional() {
        Document query = json("_id: 1, b: {$gt: 5}");
        Document update = json("$inc: {a: 1}");
        collection.updateOne(query, update, new UpdateOptions().upsert(true));
        assertThat(collection.find().first()).isEqualTo(json("_id: 1, a: 1"));
    }

    @Test
    public void testUpsertWithEmbeddedQuery() {
        collection.updateOne(json("_id: 1, 'e.i': 1"), json("$set: {a:1}"), new UpdateOptions().upsert(true));
        assertThat(collection.find(json("_id: 1")).first()).isEqualTo(json("_id:1, e: {i:1}, a:1"));
    }

    @Test
    public void testUpsertWithIdIn() throws Exception {
        Document query = json("_id: {$in: [1]}");
        Document update = json("$push: {n: {_id: 2 ,u : 3}}, $inc: {c: 4}");
        Document expected = json("_id: 1, n: [{_id: 2 ,u : 3}], c: 4");

        collection.updateOne(query, update, new UpdateOptions().upsert(true));

        // the ID generation actually differs from official MongoDB which just
        // create a random object id
        Document actual = collection.find().first();
        assertThat(actual).isEqualTo(expected);
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

        try {
            collection.updateOne(object, json("$mul: {_id: 2}"));
            fail("MongoCommandException expected");
        } catch (MongoCommandException e) {
            assertThat(e.getMessage()).contains("_id");
        }

        try {
            collection.updateOne(object, json("$mul: {foo: 2}"));
            fail("MongoCommandException expected");
        } catch (MongoCommandException e) {
            assertThat(e.getMessage()).contains("cannot multiply value 'x'");
        }

        try {
            collection.updateOne(object, json("$mul: {bar: 'x'}"));
            fail("MongoCommandException expected");
        } catch (MongoCommandException e) {
            assertThat(e.getMessage()).contains("cannot multiply with non-numeric value");
        }
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
    public void testInsertsWithUniqueIndex() {
        collection.createIndex(new Document("uniqueKeyField", 1), new IndexOptions().unique(true));

        collection.insertOne(json("uniqueKeyField: 'abc1', afield: 'avalue'"));
        collection.insertOne(json("uniqueKeyField: 'abc2', afield: 'avalue'"));
        collection.insertOne(json("uniqueKeyField: 'abc3', afield: 'avalue'"));

        try {
            collection.insertOne(json("uniqueKeyField: 'abc2', afield: 'avalue'"));
            fail("MongoWriteException expected");
        } catch (MongoWriteException e) {
            assertThat(e.getMessage()).contains("duplicate key error");
        }
    }

    @Test
    public void testInsertBinaryData() throws Exception {
        collection.insertOne(new Document("test", new byte[] { 0x01, 0x02, 0x03 }));
    }

    // see https://github.com/bwaldvogel/mongo-java-server/issues/9
    @Test
    public void testUniqueIndexWithSubdocument() {
        collection.createIndex(new Document("action.actionId", 1), new IndexOptions().unique(true));

        collection.insertOne(json("action: 'abc1'"));
        collection.insertOne(json("action: { actionId: 1 }"));
        collection.insertOne(json("action: { actionId: 2 }"));
        collection.insertOne(json("action: { actionId: 3 }"));

        try {
            collection.insertOne(json("action: { actionId: 1 }"));
            fail("MongoWriteException expected");
        } catch (MongoWriteException e) {
            assertThat(e.getMessage()).contains("duplicate key error");
        }
    }

    @Test
    public void testAddNonUniqueIndexOnNonIdField() {
        collection.createIndex(new Document("someField", 1), new IndexOptions().unique(false));

        collection.insertOne(json("someField: 'abc'"));
        collection.insertOne(json("someField: 'abc'"));
    }

    @Test
    public void testCompoundUniqueIndicesNotSupportedAndThrowsException() {
        try {
            collection.createIndex(new Document("a", 1).append("b", 1), new IndexOptions().unique(true));
            fail("MongoException expected");
        } catch (MongoException e) {
            // expected
        }
    }

    @Test
    public void testCursorOptionNoTimeout() throws Exception {
        try (MongoCursor<Document> cursor = collection.find().noCursorTimeout(true).iterator()) {
            assertFalse(cursor.hasNext());
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

        try {
            collection.updateOne(object, json("$currentDate: {lastModified: null}"));
            fail("MongoCommandException expected");
        } catch (MongoCommandException e) {
            assertThat(e.getCode()).isEqualTo(2);
            assertThat(e.getErrorMessage()).startsWith("NULL").contains("is not a valid type");
        }

        try {
            collection.updateOne(object, json("$currentDate: {lastModified: 123.456}"));
            fail("MongoCommandException expected");
        } catch (MongoCommandException e) {
            assertThat(e.getCode()).isEqualTo(2);
            assertThat(e.getErrorMessage()).startsWith("Double").contains("is not a valid type");
        }

        try {
            collection.updateOne(object, json("$currentDate: {lastModified: 'foo'}"));
            fail("MongoCommandException expected");
        } catch (MongoCommandException e) {
            assertThat(e.getCode()).isEqualTo(2);
            assertThat(e.getErrorMessage()).startsWith("String").contains("is not a valid type");
        }

        try {
            collection.updateOne(object, json("$currentDate: {lastModified: {$type: 'foo'}}"));
            fail("MongoCommandException expected");
        } catch (MongoCommandException e) {
            assertThat(e.getCode()).isEqualTo(2);
            assertThat(e.getErrorMessage())
                .startsWith("The '$type' string field is required to be 'date' or 'timestamp'");
        }

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

        try {
            collection.updateOne(json("_id: 1"), json("$rename: {foo: 12345}"));
            fail("MongoCommandException expected");
        } catch (MongoCommandException e) {
            assertThat(e.getMessage()).contains("The 'to' field for $rename must be a string");
        }

        try {
            collection.updateOne(json("_id: 1"), json("$rename: {'_id': 'id'}"));
            fail("MongoCommandException expected");
        } catch (MongoCommandException e) {
            assertThat(e.getMessage()).contains("Mod on _id not allowed");
        }

        try {
            collection.updateOne(json("_id: 1"), json("$rename: {foo: '_id'}"));
            fail("MongoCommandException expected");
        } catch (MongoCommandException e) {
            assertThat(e.getMessage()).contains("Mod on _id not allowed");
        }

        try {
            collection.updateOne(json("_id: 1"), json("$rename: {foo: 'bar', 'bar': 'bar2'}"));
            fail("MongoCommandException expected");
        } catch (MongoCommandException e) {
            assertThat(e.getMessage()).contains("Cannot update 'bar' and 'bar' at the same time");
        }

        try {
            collection.updateOne(json("_id: 1"), json("$rename: {bar: 'foo', bar2: 'foo'}"));
            fail("MongoCommandException expected");
        } catch (MongoCommandException e) {
            assertThat(e.getMessage()).contains("Cannot update 'foo' and 'foo' at the same time");
        }
    }

    @Test
    public void testRenameCollection() throws Exception {
        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));
        collection.insertOne(json("_id: 3"));

        collection.renameCollection(new MongoNamespace(collection.getNamespace().getDatabaseName(), "other-collection-name"));

        Collection<String> collectionNames = toArray(db.listCollectionNames());
        assertThat(collectionNames).containsOnly("system.indexes", "other-collection-name");

        assertThat(getCollection("other-collection-name").count()).isEqualTo(3);
    }

    @Test
    public void testRenameCollection_targetAlreadyExists() throws Exception {
        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));
        collection.insertOne(json("_id: 3"));

        MongoCollection<Document> otherCollection = db.getCollection("other-collection-name");
        otherCollection.insertOne(json("_id: 1"));

        try {
            collection.renameCollection(new MongoNamespace(db.getName(), "other-collection-name"));
            fail("MongoCommandException expected");
        } catch (MongoCommandException e) {
            assertThat(e.getErrorMessage()).isEqualTo("target namespace already exists");
        }
        List<String> collectionNames = toArray(db.listCollectionNames());
        assertThat(collectionNames).containsOnly("system.indexes", collection.getNamespace().getCollectionName(),
            "other-collection-name");

        assertThat(collection.count()).isEqualTo(3);
        assertThat(getCollection("other-collection-name").count()).isEqualTo(1);
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
        assertThat(collectionNames).containsOnly("system.indexes", "other-collection-name");

        assertThat(getCollection("other-collection-name").count()).isEqualTo(3);
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

        List<Document> indexInfo = toArray(collection.listIndexes());
        assertThat(indexInfo).containsOnly( //
            json("name:'_id_', ns:'testdb.testcoll', key:{_id:1}"), //
            json("name:'_id_', ns:'testdb.other', key:{_id:1}"), //
            json("name:'bla_1', ns:'testdb.testcoll', key:{bla:1}"));
    }

    @Test
    public void testFieldSelection_deselectId() {
        collection.insertOne(json("_id: 1, order:1, visits: 2"));

        Document document = collection.find(json("{}")).projection(json("_id: 0")).first();
        assertThat(document).isEqualTo(json("order:1, visits:2"));
    }

    @Test
    public void testFieldSelection_deselectOneField() {
        Document obj = json("_id: 1, order:1, visits: 2, eid: 12345");
        collection.insertOne(obj);

        Document document = collection.find(new Document()).projection(json("visits: 0")).first();
        assertThat(document).isEqualTo(json("_id:1, order:1, eid: 12345"));
    }

    @Test
    public void testFieldSelection_deselectTwoFields() {
        Document obj = json("_id: 1, order:1, visits: 2, eid: 12345");
        collection.insertOne(obj);

        Document document = collection.find(new Document()).projection(json("visits: 0, eid: 0")).first();
        assertThat(document).isEqualTo(json("_id:1, order:1"));
    }

    @Test
    public void testFieldSelection_selectAndDeselectFields() {
        Document obj = json("_id: 1, order:1, visits: 2, eid: 12345");
        collection.insertOne(obj);

        Document document = collection.find(new Document()).projection(json("visits: 0, eid: 1")).first();
        assertThat(document).isEqualTo(json("_id:1, eid: 12345"));
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
        assertThat(document).isEqualTo(json("{_id: 1, map: {foo: 'bar'}}"));
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
        assertThat(documents).containsOnly(json("_id: 1"), json("_id: 2"), json("_id: 3"));
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

        try {
            collection.find(json("ref: {$ref: 'coll'}")).first();
            fail("MongoQueryException expected");
        } catch (MongoQueryException e) {
            assertThat(e.getCode()).isEqualTo(10068);
            assertThat(e.getMessage()).contains("invalid operator: $ref");
        }
    }

    @Test
    public void testAndOrNorWithEmptyArray() throws Exception {
        collection.insertOne(json("{}"));
        assertMongoQueryException(and());
        assertMongoQueryException(nor());
        assertMongoQueryException(or());
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
        final CountDownLatch latch = new CountDownLatch(numDocuments);
        final Queue<RuntimeException> errors = new LinkedBlockingQueue<>();
        final Semaphore concurrentOperationsOnTheFly = new Semaphore(50); // prevent MongoWaitQueueFullException

        for (int i = 1; i <= numDocuments; i++) {
            final Document document = new Document("_id", i);
            for (int j = 0; j < 10; j++) {
                document.append("key-" + i + "-" + j, "value-" + i + "-" + j);
            }
            concurrentOperationsOnTheFly.acquire();
            asyncCollection.insertOne(document, new SingleResultCallback<Void>() {
                @Override
                public void onResult(Void result, Throwable t) {
                    checkError("insert", t);
                    log.info("inserted {}", document);
                    final Document query = new Document("_id", document.getInteger("_id"));
                    asyncCollection.updateOne(query, Updates.set("updated", true), new SingleResultCallback<UpdateResult>() {
                        @Override
                        public void onResult(UpdateResult result, Throwable t) {
                            checkError("update", t);
                            log.info("updated {}: {}", query, result);
                            release();
                        }
                    });
                }

                private void checkError(String operation, Throwable t) {
                    if (t != null) {
                        log.error(operation + " of {} failed", document, t);
                        RuntimeException exception = new RuntimeException("Failed to " + operation + " " + document, t);
                        errors.add(exception);
                        release();
                        throw exception;
                    }
                }

                private void release() {
                    latch.countDown();
                    concurrentOperationsOnTheFly.release();
                }
            });
        }

        boolean success = latch.await(30, TimeUnit.SECONDS);
        assertTrue(success);

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

        long count = collection.count();
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

        List<Document> documents = toArray(collection.find(json("{ tags: { $all: [ \"appliance\", \"school\", \"book\" ] } }")));
        assertThat(documents).hasSize(2);
        assertThat(documents.get(0).get("_id")).isEqualTo(new ObjectId("5234cc89687ea597eabee675"));
        assertThat(documents.get(1).get("_id")).isEqualTo(new ObjectId("5234cc8a687ea597eabee676"));
    }

    @Test
    public void testMatchesElementQuery() throws Exception {
        collection.insertOne(json("_id: 1, results: [ 82, 85, 88 ]"));
        collection.insertOne(json("_id: 2, results: [ 75, 88, 89 ]"));

        List<Document> results = toArray(collection.find(json("results: { $elemMatch: { $gte: 80, $lt: 85 } }")));
        assertThat(results).hasSize(1);
        assertThat(results.get(0)).isEqualTo(json("\"_id\" : 1, \"results\" : [ 82, 85, 88 ]"));
    }

    @Test
    public void testIllegalElementMatchQuery() throws Exception {
        collection.insertOne(json("_id: 1, results: [ 82, 85, 88 ]"));

        try {
            collection.find(json("results: { $elemMatch: [ 85 ] }")).first();
            fail("MongoQueryException expected");
        } catch (MongoQueryException e) {
            assertThat(e.getErrorCode()).isEqualTo(2);
            assertThat(e.getErrorMessage()).isEqualTo("$elemMatch needs an Object");
        }

        try {
            collection.find(json("results: { $elemMatch: 1 }")).first();
            fail("MongoQueryException expected");
        } catch (MongoQueryException e) {
            assertThat(e.getErrorCode()).isEqualTo(2);
            assertThat(e.getErrorMessage()).isEqualTo("$elemMatch needs an Object");
        }
    }

    @Test
    public void testQueryWithComment() throws Exception {
        collection.insertOne(json("_id: 1, x: 2"));
        collection.insertOne(json("_id: 2, x: 3"));
        collection.insertOne(json("_id: 3, x: 4"));

        List<Document> documents = toArray(collection.find(json("x: { $mod: [ 2, 0 ] }, $comment: \"Find even values.\"")));
        assertThat(documents).hasSize(2);
        assertThat(documents.get(0).get("_id")).isEqualTo(1);
        assertThat(documents.get(1).get("_id")).isEqualTo(3);
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

    private void assertMongoQueryException(Bson filter) {
        try {
            collection.find(filter).first();
            fail("MongoQueryException expected");
        } catch (MongoQueryException e) {
            assertThat(e.getCode()).isEqualTo(14816);
            assertThat(e.getMessage()).contains("nonempty array");
        }
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

        long totalDocuments = collection.count();
        assertThat(totalDocuments).isEqualTo(3);

        long documentsWithY = collection.count(json("field: 'y'"));
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
        assertThat(collection.count()).isZero();
    }
}
