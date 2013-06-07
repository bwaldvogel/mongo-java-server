package de.bwaldvogel;

import static org.fest.assertions.Assertions.assertThat;
import static org.fest.assertions.Fail.fail;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.regex.Pattern;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.MongoException.DuplicateKey;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import com.mongodb.util.JSON;

import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.Constants;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;

public class MemoryBackendTest {

    private Mongo client;
    private MongoServer mongoServer;
    private DB db;
    private DBCollection collection;
    private DB admin;

    private CommandResult command(String command) {
        return admin.command(command);
    }

    private BasicDBObject json(String string) {
        string = string.trim();
        if (!string.startsWith("{")) {
            string = "{" + string + "}";
        }
        return (BasicDBObject) JSON.parse(string);
    }

    @Before
    public void setUp() throws Exception {
        mongoServer = new MongoServer(new MemoryBackend());
        InetSocketAddress serverAddress = mongoServer.bind();
        client = new MongoClient(new ServerAddress(serverAddress));
        db = client.getDB("testdb");
        admin = client.getDB("admin");
        collection = db.getCollection("testcoll");
    }

    @After
    public void tearDown() {
        client.close();
        mongoServer.shutdownNow();
    }

    @Test
    public void testUnsupportedModifier() throws Exception {
        collection.insert(json("{}"));
        try {
            collection.update(json("{}"), json("$foo: {}"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getCode()).isEqualTo(10147);
            assertThat(e.getMessage()).contains("Invalid modifier specified: $foo");
        }
    }

    @Test
    public void testAnotherUpsert() {
        DBObject query = json("_id:{ f: 'ca', 1: { l: 2 }, t: { t: 11 } }");
        DBObject update = json("'$inc': { 'n.!' : 1 , 'n.a.b:false' : 1}");

        collection.update(query, update, true, false);

        query.putAll((BSONObject) json("n: {!: 1, a: {'b:false': 1}}"));
        assertThat(collection.findOne()).isEqualTo(query);
    }

    @Test
    public void testBasicUpdate() {
        collection.insert(json("_id:1"));
        collection.insert(json("_id:2, b:5"));
        collection.insert(json("_id:3"));
        collection.insert(json("_id:4"));

        collection.update(json("_id:2"), json("_id:2, a:5"));

        assertThat(collection.findOne(json("_id:2"))).isEqualTo(json("_id:2, a:5"));
    }

    @Test
    public void testCollectionStats() throws Exception {
        CommandResult stats = collection.getStats();
        assertThat(stats.ok()).isFalse();
        assertThat(stats.getErrorMessage()).isEqualTo("ns not found");

        collection.insert(json("{}"));
        collection.insert(json("abc: 'foo'"));
        stats = collection.getStats();
        stats.throwOnError();
        assertThat(((Number) stats.get("count")).longValue()).isEqualTo(2);
        assertThat(((Number) stats.get("size")).longValue()).isEqualTo(57);
        assertThat(((Number) stats.get("avgObjSize")).doubleValue()).isEqualTo(28.5);
    }

    @Test
    public void testCompoundDateIdUpserts() {
        DBObject query = json("{ _id : { $lt : { n: 'a' , t: 10} , $gte: { n: 'a', t: 1}}}");

        List<BasicDBObject> toUpsert = Arrays.asList(json("_id: {n:'a', t: 1}"), json("_id: {n:'a', t: 2}"),
                json("_id: {n:'a', t: 3}"), json("_id: {n:'a', t: 11}"));

        for (BasicDBObject dbo : toUpsert) {
            collection.update(dbo, ((BasicDBObject) dbo.copy()).append("foo", "bar"), true, false);
        }
        List<DBObject> results = collection.find(query).toArray();
        assertThat(results).containsExactly(json("_id: {n:'a', t:1}, foo:'bar'"), //
                json("_id: {n:'a', t:2}, foo:'bar'"), //
                json("_id: {n:'a', t:3}, foo:'bar'"));
    }

    @Test
    public void testCompoundSort() {
        collection.insert(json("a:1, _id:1"));
        collection.insert(json("a:2, _id:5"));
        collection.insert(json("a:1, _id:2"));
        collection.insert(json("a:2, _id:4"));
        collection.insert(json("a:1, _id:3"));

        @SuppressWarnings("resource")
        DBCursor cursor = collection.find().sort(json("a:1, _id:-1"));
        assertThat(cursor.toArray()).containsExactly(json("a:1, _id:3"), json("a:1, _id:2"), json("a:1, _id:1"),
                json("a:2, _id:5"), json("a:2, _id:4"));
    }

    @Test
    public void testCountCommand() {
        assertThat(collection.count()).isZero();
    }

    @Test
    public void testCountWithQueryCommand() {
        collection.insert(json("n:1"));
        collection.insert(json("n:2"));
        collection.insert(json("n:2"));
        assertThat(collection.count(json("n:2"))).isEqualTo(2);
    }

    @Test
    public void testCreateIndexes() {
        collection.ensureIndex("n");
        collection.ensureIndex("b");
        List<DBObject> indexes = db.getCollection("system.indexes").find().toArray();
        assertThat(indexes).containsExactly(

        json("key:{_id:1}").append("ns", collection.getFullName()).append("name", "_id_"),
                json("key:{n:1}").append("ns", collection.getFullName()).append("name", "n_1"),
                json("key:{b:1}").append("ns", collection.getFullName()).append("name", "b_1"));
    }

    @Test
    public void testCurrentOperations() throws Exception {
        DBObject currentOperations = admin.getCollection("$cmd.sys.inprog").findOne();
        assertThat(currentOperations).isNotNull();
        assertThat(currentOperations.get("inprog")).isInstanceOf(List.class);
    }

    @Test
    public void testDatabaseStats() throws Exception {
        CommandResult stats = db.getStats();
        stats.throwOnError();
        assertThat(((Number) stats.get("objects")).longValue()).isEqualTo(1);
        assertThat(((Number) stats.get("collections")).longValue()).isEqualTo(1);
        assertThat(((Number) stats.get("indexes")).longValue()).isEqualTo(0);
        assertThat(((Number) stats.get("dataSize")).longValue()).isEqualTo(37);

        db.getCollection("foo").insert(json("{}"));
        db.getCollection("foo").insert(json("{}"));
        db.getCollection("bar").insert(json("{}"));

        stats = db.getStats();
        stats.throwOnError();

        assertThat(((Number) stats.get("objects")).longValue()).isEqualTo(8);
        assertThat(((Number) stats.get("collections")).longValue()).isEqualTo(3);
        assertThat(((Number) stats.get("indexes")).longValue()).isEqualTo(2);
        assertThat(((Number) stats.get("dataSize")).longValue()).isEqualTo(271);
    }

    @Test
    public void testDeleteDecrementsCount() {
        collection.insert(json("key: 'value'"));
        assertThat(collection.count()).isEqualTo(1);
        collection.remove(json("{}"));
        assertThat(collection.count()).isZero();
    }

    @Test
    public void testDeleteInSystemNamespace() throws Exception {
        try {
            db.getCollection("system.foobar").remove(json("{}"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getCode()).isEqualTo(12050);
            assertThat(e.getMessage()).contains("cannot delete from system namespace");
        }

        try {
            db.getCollection("system.namespaces").remove(json("{}"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getCode()).isEqualTo(12050);
            assertThat(e.getMessage()).contains("cannot delete from system namespace");
        }
    }

    @Test
    public void testUpdateInSystemNamespace() throws Exception {
        try {
            db.getCollection("system.foobar").update(json("{}"), json("{}"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getCode()).isEqualTo(10156);
            assertThat(e.getMessage()).contains("cannot update system collection");
        }

        try {
            db.getCollection("system.namespaces").update(json("{}"), json("{}"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getCode()).isEqualTo(10156);
            assertThat(e.getMessage()).contains("cannot update system collection");
        }
    }

    @Test
    public void testDistinctQuery() {
        collection.insert(new BasicDBObject("n", 3));
        collection.insert(new BasicDBObject("n", 1));
        collection.insert(new BasicDBObject("n", 2));
        collection.insert(new BasicDBObject("n", 1));
        collection.insert(new BasicDBObject("n", 1));
        assertThat(collection.distinct("n")).containsExactly(1, 2, 3);
        assertThat(collection.distinct("foobar")).isEmpty();
        assertThat(collection.distinct("_id")).hasSize((int) collection.count());
    }

    @Test
    public void testDropCollection() throws Exception {
        collection.insert(json("{}"));
        assertThat(db.getCollectionNames()).contains(collection.getName());
        collection.drop();
        assertThat(db.getCollectionNames()).excludes(collection.getName());
    }

    @Test
    public void testDropCollectionAlsoDropsFromDB() throws Exception {
        collection.insert(json("{}"));
        collection.drop();
        assertThat(collection.count()).isZero();
        assertThat(db.getCollectionNames()).excludes(collection.getName());
    }

    @Test
    public void testDropDatabaseAlsoDropsCollectionData() throws Exception {
        collection.insert(json("{}"));
        db.dropDatabase();
        assertThat(collection.count()).isZero();
    }

    @Test
    public void testDropDatabaseDropsAllData() throws Exception {
        collection.insert(json("{}"));
        DBCollection collection2 = db.getCollection("testcoll2");
        collection2.insert(json("{}"));

        client.dropDatabase(db.getName());
        assertThat(client.getDatabaseNames()).excludes(db.getName());
        assertThat(collection.count()).isZero();
        assertThat(db.getCollectionNames()).excludes(collection.getName(), collection2.getName());
    }

    @Test
    public void testEmbeddedSort() {
        collection.insert(json("_id: 1"));
        collection.insert(json("_id: 2"));
        collection.insert(json("_id: 3"));
        collection.insert(json("_id: 4, counts:{done:1}"));
        collection.insert(json("_id: 5, counts:{done:2}"));

        @SuppressWarnings("resource")
        DBCursor cursor = collection.find(json("c: {$ne:true}")).sort(json("counts.done: -1"));
        assertThat(cursor.toArray()).containsExactly(json("_id: 5, counts:{done:2}"), json("_id: 4, counts:{done:1}"),
                json("_id: 1"), json("_id: 2"), json("_id: 3"));
    }

    @Test
    public void testFindAndModifyCommandEmpty() throws Exception {
        DBObject cmd = new BasicDBObject("findandmodify", collection.getName());
        CommandResult result = db.command(cmd);
        assertThat(result.getErrorMessage()).isEqualTo("need remove or update");
        assertThat(result.ok()).isFalse();
    }

    @Test
    public void testFindAndModifyCommandIllegalOp() throws Exception {
        collection.insert(json("_id: 1"));

        DBObject cmd = new BasicDBObject("findAndModify", collection.getName());
        cmd.put("query", json("_id: 1"));
        cmd.put("update", new BasicDBObject("$inc", json("_id: 1")));

        assertThat(collection.findOne()).isEqualTo(json("_id: 1"));

        CommandResult result = db.command(cmd);
        try {
            result.throwOnError();
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getCode()).isEqualTo(10148);
            assertThat(e.getMessage()).contains("Mod on _id not allowed");
        }
    }

    @Test
    public void testFindAndModifyCommandUpdate() throws Exception {
        collection.insert(json("_id: 1"));

        DBObject cmd = new BasicDBObject("findAndModify", collection.getName());
        cmd.put("query", json("_id: 1"));
        cmd.put("update", json("$inc: {a: 1}"));

        CommandResult result = db.command(cmd);
        assertThat(result.get("lastErrorObject")).isEqualTo(json("updatedExisting: true, n: 1"));

        assertThat(collection.findOne()).isEqualTo(json("_id: 1, a: 1"));
        assertThat(result.ok()).isTrue();
    }

    @Test
    public void testFindAndModifyError() throws Exception {
        collection.insert(json("_id: 1, a: 1"));

        try {
            collection.findAndModify(json("_id: 1"), null, null, false, json("$inc: {_id: 1}"), false, false);
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getCode()).isEqualTo(10148);
            assertThat(e.getMessage()).contains("Mod on _id not allowed");
        }
    }

    @Test
    public void testFindAndModifyFields() throws Exception {
        collection.insert(json("_id: 1, a: 1"));
        DBObject result = collection.findAndModify(json("_id: 1"), json("_id: 1"), null, false, json("$inc: {a:1}"),
                true, false);

        assertThat(result).isEqualTo(json("_id: 1"));
    }

    @Test
    public void testFindAndModifyNotFound() throws Exception {
        collection.insert(json("_id: 1, a: 1"));
        DBObject result = collection.findAndModify(json("_id: 2"), null, null, false, new BasicDBObject("$inc",
                json("a: 1")), false, false);

        assertThat(result).isNull();
        assertThat(collection.count()).isEqualTo(1);
    }

    @Test
    public void testFindAndModifyRemove() {
        collection.insert(json("_id: 1, a: 1"));
        DBObject result = collection.findAndModify(json("_id: 1"), null, null, true, null, false, false);

        assertThat(result).isEqualTo(json("_id: 1, a: 1"));
        assertThat(collection.count()).isZero();
    }

    // https://github.com/foursquare/fongo/issues/32
    @Test
    public void testFindAndModifyReturnNew() {
        collection.insert(json("_id: 1, a: 1, b: {c: 1}"));

        DBObject query = json("_id: 1");
        DBObject update = json("$inc: {a: 1, 'b.c': 1}");
        DBObject result = collection.findAndModify(query, null, null, false, update, true, false);

        assertThat(result).isEqualTo(json("_id: 1, a: 2, b: {c: 2}"));
    }

    // https://github.com/foursquare/fongo/issues/32
    @Test
    public void testFindAndModifyReturnOld() {
        collection.insert(json("_id: 1, a: 1, b: {c: 1}"));

        DBObject query = json("_id: 1");
        DBObject update = json("$inc: {a: 1, 'b.c': 1}");
        DBObject result = collection.findAndModify(query, null, null, false, update, false, false);

        assertThat(result).isEqualTo(json("_id: 1, a: 1, b: {c: 1}"));
        assertThat(collection.findOne(query)).isEqualTo(json("_id: 1, a: 2, b: {c: 2}"));
    }

    @Test
    public void testFindAndModifySorted() throws Exception {
        collection.insert(json("_id: 1, a:15"));
        collection.insert(json("_id: 2, a:10"));
        collection.insert(json("_id: 3, a:20"));

        DBObject order = json("a:1");
        DBObject result = collection.findAndModify(json("{}"), null, order, false, json("$inc: {a: 1}"), true, false);
        assertThat(result).isEqualTo(json("_id: 2, a: 11"));

        order = json("a: -1");
        result = collection.findAndModify(json("{}"), null, order, false, json("$inc: {a: 1}"), true, false);
        assertThat(result).isEqualTo(json("_id: 3, a: 21"));

    }

    @Test
    public void testFindAndModifyUpsert() {
        DBObject result = collection.findAndModify(json("_id: 1"), null, null, false, json("$inc: {a:1}"), true, true);

        assertThat(result).isEqualTo(json("_id: 1, a: 1"));
        assertThat(collection.findOne()).isEqualTo(json("_id: 1, a: 1"));
    }

    @Test
    public void testFindAndModifyUpsertReturnNewFalse() {
        DBObject result = collection.findAndModify(json("_id: 1"), null, null, false, json("$inc: {a:1}"), false, true);

        assertThat(result).isEqualTo(json("{}"));
        assertThat(collection.findOne()).isEqualTo(json("_id: 1, a: 1"));
    }

    @Test
    public void testFindAndRemoveFromEmbeddedList() {
        BasicDBObject obj = json("_id: 1, a: [1]");
        collection.insert(obj);
        DBObject result = collection.findAndRemove(json("_id: 1"));
        assertThat(result).isEqualTo(obj);
        assertThat(collection.count()).isZero();
    }

    @Test
    public void testFindOne() {
        collection.insert(json("key: 'value'"));
        collection.insert(json("key: 'value'"));
        DBObject result = collection.findOne();
        assertThat(result).isNotNull();
        assertThat(result.get("_id")).isNotNull();
    }

    @Test
    public void testFindOneById() {
        collection.insert(json("_id: 1"));
        DBObject result = collection.findOne(json("_id: 1"));
        assertThat(result).isEqualTo(json("_id: 1"));
        assertThat(collection.findOne(json("_id: 2"))).isNull();
    }

    @Test
    public void testFindOneIn() {
        collection.insert(json("_id: 1"));
        DBObject result = collection.findOne(json("_id: {$in: [1,2]}"));
        assertThat(result).isEqualTo(json("_id: 1"));
    }

    @Test
    public void testFindWithLimit() {
        collection.insert(json("_id: 1"));
        collection.insert(json("_id: 2"));
        collection.insert(json("_id: 3"));
        collection.insert(json("_id: 4"));

        List<DBObject> actual = collection.find().limit(2).toArray();
        assertThat(actual).containsExactly(json("_id: 1"), json("_id: 2"));

        List<DBObject> actualNegativeLimit = collection.find().limit(-2).toArray();
        assertThat(actualNegativeLimit).isEqualTo(actual);
    }

    @Test
    public void testFindWithPattern() {
        collection.insert(json("_id: 'marta'"));
        collection.insert(json("_id: 'john', foo: 'bar'"));
        collection.insert(json("_id: 'jon', foo: 'ba'"));
        collection.insert(json("_id: 'jo'"));

        assertThat(collection.find(new BasicDBObject("_id", Pattern.compile("mart"))).toArray()).containsExactly(
                json("_id: 'marta'"));

        assertThat(collection.find(new BasicDBObject("foo", Pattern.compile("ba"))).toArray()).containsExactly(
                json("_id: 'john', foo: 'bar'"), json("_id: 'jon', foo: 'ba'"));

        assertThat(collection.find(new BasicDBObject("foo", Pattern.compile("ba$"))).toArray()).containsExactly(
                json("_id: 'jon', foo: 'ba'"));
    }

    @Test
    public void testFindWithQuery() {
        collection.insert(json("name: 'jon'"));
        collection.insert(json("name: 'leo'"));
        collection.insert(json("name: 'neil'"));
        collection.insert(json("name: 'neil'"));

        @SuppressWarnings("resource")
        DBCursor cursor = collection.find(json("name: 'neil'"));
        assertThat(cursor.toArray()).hasSize(2);
    }

    @Test
    public void testFindWithSkipLimit() {
        collection.insert(json("_id: 1"));
        collection.insert(json("_id: 2"));
        collection.insert(json("_id: 3"));
        collection.insert(json("_id: 4"));

        @SuppressWarnings("resource")
        DBCursor cursor = collection.find().limit(2).skip(2);
        assertThat(cursor.toArray()).containsExactly(json("_id: 3"), json("_id: 4"));
    }

    @Test
    public void testFindWithSkipLimitAfterDelete() {
        collection.insert(json("_id: 1"));
        collection.insert(json("_id: 2"));
        collection.insert(json("_id: 3"));
        collection.insert(json("_id: 4"));
        collection.insert(json("_id: 5"));

        collection.remove(json("_id: 1"));
        collection.remove(json("_id: 3"));

        @SuppressWarnings("resource")
        DBCursor cursor = collection.find().limit(2).skip(2);
        assertThat(cursor.toArray()).containsExactly(json("_id: 5"));
    }

    @Test
    public void testFullUpdateWithSameId() throws Exception {
        collection.insert(json("_id: 1"));
        collection.insert(json("_id: 2, b: 5"));
        collection.insert(json("_id: 3"));
        collection.insert(json("_id: 4"));

        collection.update(json("_id: 2, b:5"), json("_id: 2, a:5"));

        assertThat(collection.findOne(json("_id: 2"))).isEqualTo(json("_id: 2, a:5"));
    }

    @Test
    public void testGetCollection() {
        DBCollection collection = db.getCollection("coll");
        db.getCollection("coll").insert(json("{}"));

        assertThat(collection).isNotNull();
        assertThat(db.getCollection("coll")).isSameAs(collection);
        assertThat(db.getCollectionFromString("coll")).isSameAs(collection);
        assertThat(db.getCollectionNames()).contains("coll");
    }

    @Test
    public void testGetLastError() {

        assertThat(db.getLastError().get("err")).isNull();

        WriteResult result = collection.insert(json("_id: 1"));
        CommandResult error = db.getLastError();
        assertThat(error.ok()).isTrue();
        assertThat(error).isEqualTo(result.getCachedLastError());

        assertThat(db.getLastError()).isEqualTo(error);

        try {
            collection.insert(json("_id: 1"));
            fail("DuplicateKey expected");
        } catch (DuplicateKey e) {
            // okay
        }

        // getlasterror must show the error
        CommandResult lastError = db.getLastError();
        assertThat(lastError.getString("err")).startsWith("duplicate key error");
        assertThat(db.getLastError()).isEqualTo(lastError);

        collection.findOne(json("{}"));
        assertThat(db.getLastError().getString("err")).isNull();
    }

    @Test
    public void testGetPreviousError() {

        assertThat(db.getPreviousError().getString("err")).isNull();
        assertThat(db.getPreviousError().getInt("nPrev")).isEqualTo(-1);

        collection.insert(json("_id: 1"));
        try {
            collection.insert(json("_id: 1"));
            fail("DuplicateKey expected");
        } catch (DuplicateKey e) {
            // okay
        }

        // getlasterror must show the error
        CommandResult lastError = db.getLastError();
        assertThat(lastError.getString("err")).startsWith("duplicate key error");
        CommandResult previousError = db.getPreviousError();
        assertThat(previousError).isEqualTo(db.getLastError().append("nPrev", 1));
        assertThat(db.getPreviousError()).isEqualTo(previousError);

        collection.findOne(json("{}"));
        assertThat(db.getLastError().getString("err")).isNull();
        assertThat(db.getPreviousError()).isEqualTo(lastError.append("nPrev", 2));

        collection.findOne(json("{}"));
        assertThat(db.getPreviousError()).isEqualTo(lastError.append("nPrev", 3));

        collection.update(json("{}"), json("a:1"));
        assertThat(db.getPreviousError().getInt("nPrev")).isEqualTo(1);
        assertThat(db.getPreviousError().getInt("n")).isEqualTo(1);

    }

    @Test
    public void testResetError() {
        collection.insert(json("_id: 1"));
        try {
            collection.insert(json("_id: 1"));
            fail("DuplicateKey expected");
        } catch (DuplicateKey e) {
            // okay
        }

        // getlasterror must show the error
        CommandResult lastError = db.getLastError();
        assertThat(lastError.getString("err")).startsWith("duplicate key error");
        CommandResult previousError = db.getPreviousError();
        assertThat(previousError).isEqualTo(db.getLastError().append("nPrev", 1));

        db.resetError();
        assertThat(db.getLastError().get("err")).isNull();
        assertThat(db.getPreviousError().get("err")).isNull();
        assertThat(db.getPreviousError().getInt("nPrev")).isEqualTo(-1);

    }

    /**
     * Test that ObjectId is getting generated even if _id is present in
     * DBObject but it's value is null
     */
    @Test
    public void testIdGenerated() throws Exception {
        DBObject toSave = json("{_id: null, name: 'test'}");

        collection.save(toSave);
        DBObject result = collection.findOne(json("name: 'test'"));
        assertThat(result.get(Constants.ID_FIELD)).isInstanceOf(ObjectId.class);
    }

    @Test
    public void testIdInQueryResultsInIndexOrder() {
        collection.insert(json("_id: 4"));
        collection.insert(json("_id: 3"));
        collection.insert(json("_id: 1"));
        collection.insert(json("_id: 2"));

        @SuppressWarnings("resource")
        DBCursor cursor = collection.find(json("_id: {$in: [3,2,1]}"));
        assertThat(cursor.toArray()).containsExactly(json("_id: 1"), json("_id: 2"), json("_id: 3"));
    }

    @Test
    public void testIdNotAllowedToBeUpdated() {
        collection.insert(json("_id: 1"));

        try {
            collection.update(json("_id: 1"), json("_id:2, a:4"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getMessage()).contains(
                    "cannot change _id of a document old:{ \\\"_id\\\" : 1} new:{ \\\"_id\\\" : 2}");
        }

        // test with $set

        try {
            collection.update(json("_id: 1"), new BasicDBObject("$set", json("_id: 2")));
            fail("should throw exception");
        } catch (MongoException e) {
            assertThat(e.getMessage()).contains("Mod on _id not allowed");
        }
    }

    @Test
    public void testIllegalCommand() throws Exception {
        try {
            command("foo").throwOnError();
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getMessage()).contains("no such cmd: foo");
        }

        try {
            client.getDB("bar").command("foo").throwOnError();
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getMessage()).contains("no such cmd: foo");
        }
    }

    @Test
    public void testInsert() throws Exception {
        assertThat(collection.count()).isEqualTo(0);

        for (int i = 0; i < 3; i++) {
            collection.insert(new BasicDBObject("_id", Integer.valueOf(i)));
        }

        assertThat(collection.count()).isEqualTo(3);

        WriteResult result = collection.insert(json("foo: [1,2,3]"));
        assertThat(result.getN()).isZero();
        assertThat(result.getField("updatedExisting")).isNull();

        collection.insert(new BasicDBObject("foo", new byte[10]));
        BasicDBObject insertedObject = new BasicDBObject("foo", UUID.randomUUID());
        collection.insert(insertedObject);
        assertThat(collection.findOne(insertedObject)).isEqualTo(insertedObject);
    }

    @Test
    public void testInsertDuplicate() throws Exception {
        assertThat(collection.count()).isEqualTo(0);

        collection.insert(json("_id: 1"));
        assertThat(collection.count()).isEqualTo(1);

        try {
            collection.insert(json("_id: 1"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getMessage()).contains("duplicate key error");
        }

        try {
            collection.insert(new BasicDBObject("_id", Double.valueOf(1.0)));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getMessage()).contains("duplicate key error");
        }

        assertThat(collection.count()).isEqualTo(1);
    }

    @Test(expected = MongoException.DuplicateKey.class)
    public void testInsertDuplicateThrows() {
        collection.insert(json("_id: 1"));
        collection.insert(json("_id: 1"));
    }

    @Test(expected = MongoException.DuplicateKey.class)
    public void testInsertDuplicateWithConcernThrows() {
        collection.insert(json("_id: 1"));
        collection.insert(json("_id: 1"), WriteConcern.SAFE);
    }

    @Test
    public void testInsertIncrementsCount() {
        assertThat(collection.count()).isZero();
        collection.insert(json("key: 'value'"));
        assertThat(collection.count()).isEqualTo(1);
    }

    @Test
    public void testInsertQuery() throws Exception {
        assertThat(collection.count()).isEqualTo(0);

        BasicDBObject insertedObject = json("_id: 1");
        insertedObject.put("foo", "bar");
        collection.insert(insertedObject);

        assertThat(collection.findOne(insertedObject)).isEqualTo(insertedObject);
        assertThat(collection.findOne(new BasicDBObject("_id", Long.valueOf(1)))).isEqualTo(insertedObject);
        assertThat(collection.findOne(new BasicDBObject("_id", Double.valueOf(1.0)))).isEqualTo(insertedObject);
        assertThat(collection.findOne(new BasicDBObject("_id", Float.valueOf(1.0001f)))).isNull();
        assertThat(collection.findOne(json("foo: 'bar'"))).isEqualTo(insertedObject);
    }

    @Test
    public void testInsertRemove() throws Exception {
        for (int i = 0; i < 10; i++) {
            collection.insert(json("_id: 1"));
            assertThat(collection.count()).isEqualTo(1);
            collection.remove(json("_id: 1"));
            assertThat(collection.count()).isZero();

            collection.insert(new BasicDBObject("_id", i));
            collection.remove(new BasicDBObject("_id", i));
        }
        assertThat(collection.count()).isZero();
        collection.remove(json("'doesnt exist': 1"));
        assertThat(collection.count()).isZero();
    }

    @Test
    public void testInsertInSystemNamespace() throws Exception {
        try {
            db.getCollection("system.foobar").insert(json("{}"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getCode()).isEqualTo(16459);
            assertThat(e.getMessage()).contains("attempt to insert in system namespace");
        }

        try {
            db.getCollection("system.namespaces").insert(json("{}"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getCode()).isEqualTo(16459);
            assertThat(e.getMessage()).contains("attempt to insert in system namespace");
        }
    }

    @Test
    public void testListDatabaseNames() throws Exception {
        assertThat(client.getDatabaseNames()).isEmpty();
        db.getCollection(collection.getName()).insert(json("{}"));
        assertThat(client.getDatabaseNames()).containsExactly(db.getName());
        client.getDB("bar").getCollection(collection.getName()).insert(json("{}"));
        assertThat(client.getDatabaseNames()).containsExactly("bar", db.getName());
    }

    @Test
    public void testMaxBsonSize() throws Exception {
        int maxBsonObjectSize = client.getMaxBsonObjectSize();
        assertThat(maxBsonObjectSize).isEqualTo(16777216);
    }

    @Test
    public void testQuery() throws Exception {
        DBObject obj = collection.findOne(json("_id: 1"));
        assertThat(obj).isNull();
        assertThat(collection.count()).isEqualTo(0);
    }

    @Test
    public void testQueryAll() throws Exception {
        List<Object> inserted = new ArrayList<Object>();
        for (int i = 0; i < 10; i++) {
            BasicDBObject obj = new BasicDBObject("_id", i);
            collection.insert(obj);
            inserted.add(obj);
        }
        assertThat(collection.count()).isEqualTo(10);

        assertThat(collection.find().toArray()).isEqualTo(inserted);
    }

    @Test
    public void testQueryCount() throws Exception {
        for (int i = 0; i < 100; i++) {
            collection.insert(json("{}"));
        }
        assertThat(collection.count()).isEqualTo(100);

        BasicDBObject obj = json("_id: 1");
        assertThat(collection.find(obj).count()).isEqualTo(0);
        collection.insert(obj);
        assertThat(collection.find(obj).count()).isEqualTo(1);
    }

    @Test
    public void testQueryLimit() throws Exception {
        for (int i = 0; i < 5; i++) {
            collection.insert(json("{}"));
        }
        List<DBObject> objects = collection.find().limit(1).toArray();
        assertThat(objects.size()).isEqualTo(1);

        assertThat(collection.find().limit(-1).toArray()).isEqualTo(objects);
    }

    @Test
    public void testQueryNull() throws Exception {
        BasicDBObject object = json("_id: 1");
        collection.insert(object);
        assertThat(collection.findOne(json("foo: null"))).isEqualTo(object);
    }

    @Test
    @SuppressWarnings("resource")
    public void testQuerySkipLimit() throws Exception {
        for (int i = 0; i < 10; i++) {
            collection.insert(json("{}"));
        }

        DBCursor cursor = collection.find().skip(3);
        assertThat(cursor.itcount()).isEqualTo(7);

        cursor = collection.find().skip(3).limit(5);
        assertThat(cursor.itcount()).isEqualTo(5);
    }

    @Test
    public void testQuerySort() throws Exception {
        Random random = new Random(4711);
        for (int i = 0; i < 10; i++) {
            collection.insert(new BasicDBObject("_id", Double.valueOf(random.nextDouble())));
        }

        List<DBObject> objects = collection.find().sort(json("_id: 1")).toArray();
        double before = Double.MIN_VALUE;
        for (DBObject dbObject : objects) {
            double value = ((Number) dbObject.get("_id")).doubleValue();
            assertThat(value).isGreaterThanOrEqualTo(before);
            before = value;
        }

        // reverse sort
        objects = collection.find().sort(json("_id: -1")).toArray();
        before = Double.MAX_VALUE;
        for (DBObject dbObject : objects) {
            double value = ((Number) dbObject.get("_id")).doubleValue();
            assertThat(value).isLessThanOrEqualTo(before);
            before = value;
        }
    }

    @Test
    public void testQueryWithFieldSelector() throws Exception {
        collection.insert(json("foo: 'bar'"));
        DBObject obj = collection.findOne(json("{}"), json("foo: 1"));
        assertThat(obj.keySet()).containsOnly("_id", "foo");

        obj = collection.findOne(json("foo:'bar'"), json("_id: 1"));
        assertThat(obj.keySet()).containsOnly("_id");

        obj = collection.findOne(json("foo: 'bar'"), json("_id: 0, foo:1"));
        assertThat(obj.keySet()).containsOnly("foo");
    }

    @Test
    public void testQuerySystemNamespace() throws Exception {
        assertThat(db.getCollection("system.foobar").findOne()).isNull();
        assertThat(db.getCollectionNames()).containsOnly("system.indexes");

        collection.insert(json("{}"));
        BasicDBObject expectedObj = new BasicDBObject("name", collection.getFullName());
        DBObject coll = db.getCollection("system.namespaces").findOne(expectedObj);
        assertThat(coll).isEqualTo(expectedObj);
    }

    @Test
    public void testQueryAllExpression() throws Exception {
        collection.insert(json(" _id : [ { x : 1 } , { x : 2  } ]"));
        collection.insert(json(" _id : [ { x : 2 } , { x : 3  } ]"));

        assertThat(collection.find(json("'_id.x':{$all:[1,2]}")).toArray()).hasSize(1);
        assertThat(collection.find(json("'_id.x':{$all:[2,3]}")).toArray()).hasSize(1);
    }

    @Test
    public void testRemove() {
        collection.insert(json("_id: 1"));
        collection.insert(json("_id: 2"));
        collection.insert(json("_id: 3"));
        collection.insert(json("_id: 4"));

        collection.remove(json("_id: 2"));

        assertThat(collection.findOne(json("_id: 2"))).isNull();
        assertThat(collection.count()).isEqualTo(3);

        collection.remove(json("_id: {$gte: 3}"));
        assertThat(collection.count()).isEqualTo(1);
        assertThat(collection.findOne()).isEqualTo(json("_id: 1"));
    }

    @Test
    public void testRemoveSingle() throws Exception {
        DBObject obj = new BasicDBObject("_id", ObjectId.get());
        collection.insert(obj);
        collection.remove(obj);
    }

    @Test
    public void testRemoveReturnsModifiedDocumentCount() {
        collection.insert(json("{}"));
        collection.insert(json("{}"));

        WriteResult result = collection.remove(json("{}"));
        assertThat(result.getN()).isEqualTo(2);

        result = collection.remove(json("{}"));
        assertThat(result.getN()).isEqualTo(0);

        assertThat(result.getError()).isNull();
    }

    @Test
    public void testReservedCollectionNames() throws Exception {
        try {
            db.getCollection("foo$bar").insert(json("{}"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getMessage()).contains("cannot insert into reserved $ collection");
        }

        try {
            db.getCollection("").insert(json("{}"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getMessage().toLowerCase()).contains("invalid ns");
        }

        try {
            db.getCollection(
                    "verylongstringverylongstringverylongstringverylongstringverylongstringverylongstringverylongstringverylongstringverylongstringverylongstringverylongstringverylongstringverylongstringverylongstringverylongstringverylongstring")
                    .insert(json("{}"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getMessage()).contains("name too long");
        }
    }

    @Test
    public void testSave() {
        BasicDBObject inserted = json("_id: 1");
        collection.insert(inserted);
        collection.save(inserted);
    }

    @Test
    public void testServerStatus() throws Exception {
        Date before = new Date();
        CommandResult serverStatus = command("serverStatus");
        serverStatus.throwOnError();
        assertThat(serverStatus.get("uptime")).isInstanceOf(Number.class);
        assertThat(serverStatus.get("uptimeMillis")).isInstanceOf(Long.class);
        Date serverTime = (Date) serverStatus.get("localTime");
        assertThat(serverTime).isNotNull();
        assertThat(serverTime.after(new Date())).isFalse();
        assertThat(before.after(serverTime)).isFalse();

        BSONObject connections = (BSONObject) serverStatus.get("connections");
        assertThat(connections.get("current")).isEqualTo(Integer.valueOf(1));
    }

    @Test
    public void testReplSetGetStatus() throws Exception {
        CommandResult result = command("replSetGetStatus");
        assertThat(result.ok()).isFalse();
        assertThat(result.getErrorMessage()).isEqualTo("not running with --replSet");
    }

    @Test
    public void testWhatsMyUri() throws Exception {
        for (String dbname : new String[] { "admin", "local", "test" }) {
            CommandResult result = client.getDB(dbname).command("whatsmyuri");
            result.throwOnError();
            assertThat(result.ok()).isTrue();
            assertThat(result.get("you")).isNotNull();
            assertThat(result.get("you").toString()).startsWith("127.0.0.1:");
        }
    }

    @Test
    public void testSort() {
        collection.insert(json("a:1, _id:1"));
        collection.insert(json("a:2, _id:2"));
        collection.insert(json("_id: 5"));
        collection.insert(json("a:3, _id:3"));
        collection.insert(json("a:4, _id:4"));

        @SuppressWarnings("resource")
        DBCursor cursor = collection.find().sort(json("a: -1"));
        assertThat(cursor.toArray()).containsExactly(json("a:4, _id:4"), json("a:3, _id:3"), json("a:2, _id:2"),
                json("a:1, _id:1"), json("_id: 5"));
    }

    @Test
    public void testSortByEmbeddedKey() {
        collection.insert(json("_id: 1, a: { b:1 }"));
        collection.insert(json("_id: 2, a: { b:2 }"));
        collection.insert(json("_id: 3, a: { b:3 }"));
        List<DBObject> results = collection.find().sort(json("'a.b': -1")).toArray();
        assertThat(results).containsExactly(json("_id: 3, a: { b:3 }"), json("_id: 2, a: { b:2 }"),
                json("_id: 1, a: { b:1 }"));
    }

    @Test
    public void testUpdate() throws Exception {
        BasicDBObject object = json("_id: 1");

        BasicDBObject newObject = json("_id: 1");
        newObject.put("foo", "bar");

        collection.insert(object);
        WriteResult result = collection.update(object, newObject);
        assertThat(result.getN()).isEqualTo(1);
        assertThat(result.getField("updatedExisting")).isEqualTo(Boolean.TRUE);
        assertThat(result.getField("upserted")).isNull();
        assertThat(collection.findOne(object)).isEqualTo(newObject);
    }

    @Test
    public void testUpdateNothing() throws Exception {
        BasicDBObject object = json("_id: 1");
        WriteResult result = collection.update(object, object);
        assertThat(result.getN()).isEqualTo(0);
        assertThat(result.getField("updatedExisting")).isEqualTo(Boolean.FALSE);
    }

    @Test
    public void testUpdateBlank() throws Exception {
        DBObject document = json("'': 1, _id: 2, a: 3, b: 4");
        collection.insert(document);

        collection.update(json("{}"), json("$set: {c:5}"));
        assertThat(collection.findOne()).isEqualTo(json("'': 1, _id: 2, a: 3, b: 4, c:5"));
    }

    @Test
    public void testUpdateEmptyPositional() throws Exception {
        try {
            collection.update(json("{}"), json("$set:{'a.$.b': 1}"), true, false);
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getCode()).isEqualTo(16650);
            assertThat(e.getMessage()).contains(
                    "Cannot apply the positional operator without a corresponding query field containing an array.");
        }
    }

    @Test
    public void testUpdateMultiplePositional() throws Exception {
        collection.insert(json("{a: {b: {c: 1}}}"));
        try {
            collection.update(json("{'a.b.c':1}"), json("$set:{'a.$.b.$.c': 1}"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getCode()).isEqualTo(16650);
            assertThat(e.getMessage()).contains(
                    "Cannot apply the positional operator without a corresponding query field containing an array.");
        }
    }

    @Test
    public void testUpdateIllegalFieldName() throws Exception {

        // Disallow $ in field names - SERVER-3730

        collection.insert(json("{x:1}"));

        collection.update(json("{x:1}"), json("$set: {y:1}")); // ok

        try {
            collection.update(json("{x:1}"), json("$set: {$z:1}"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getCode()).isEqualTo(15896);
            assertThat(e.getMessage()).contains("Modified field name may not start with $");
        }

        // unset ok to remove bad fields
        collection.update(json("{x:1}"), json("$unset: {$z:1}"));

        try {
            collection.update(json("{x:1}"), json("$inc: {$z:1}"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getCode()).isEqualTo(15896);
            assertThat(e.getMessage()).contains("Modified field name may not start with $");
        }

        try {
            collection.update(json("{x:1}"), json("$pushAll: {$z:[1,2,3]}"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getCode()).isEqualTo(15896);
            assertThat(e.getMessage()).contains("Modified field name may not start with $");
        }

    }

    @Test
    public void testUpdateSubdocument() throws Exception {
        try {
            collection.update(json("{}"), json("'a.b.c': 123"));
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).contains("Bad Key");
        }
    }

    @Test
    public void testUpdateIdNoChange() {
        collection.insert(json("_id: 1"));
        collection.update(json("_id: 1"), json("_id: 1, a: 5"));

        assertThat(collection.findOne(json("_id: 1"))).isEqualTo(json("_id: 1, a: 5"));

        collection.update(json("_id: 1"), json("$set: {_id: 1, b: 3}"));

        assertThat(collection.findOne(json("_id: 1"))).isEqualTo(json("_id: 1, a: 5, b: 3"));

        // test with $set

        collection.update(json("_id: 1"), json("$set: {_id: 1, a: 7}"));

        assertThat(collection.findOne(json("_id: 1"))).isEqualTo(json("_id: 1, a: 7, b: 3"));
    }

    @Test
    public void testUpdatePush() throws Exception {
        BasicDBObject idObj = json("_id: 1");
        collection.insert(idObj);
        collection.update(idObj, json("$push: {'field.subfield.subsubfield': 'value'}"));
        DBObject expected = json("_id: 1, field:{subfield:{subsubfield: ['value']}}");
        assertThat(collection.findOne(idObj)).isEqualTo(expected);

        // push to non-array
        collection.update(idObj, json("$set: {field: 'value'}"));
        try {
            collection.update(idObj, json("$push: {field: 'value'}"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getCode()).isEqualTo(10141);
            assertThat(e.getMessage()).contains("Cannot apply $push modifier to non-array");
        }

        // push with multiple fields

        DBObject pushObj = json("$push: {field1: 'value', field2: 'value2'}");
        collection.update(idObj, pushObj);

        expected = json("_id: 1, field: 'value', field1: ['value'], field2: ['value2']");
        assertThat(collection.findOne(idObj)).isEqualTo(expected);

        // push duplicate
        pushObj = json("$push: {field1: 'value'}");
        collection.update(idObj, pushObj);
        expected.put("field1", Arrays.asList("value", "value"));
        assertThat(collection.findOne(idObj)).isEqualTo(expected);
    }

    @Test
    public void testUpdatePushAll() throws Exception {
        DBObject idObj = json("_id: 1");
        collection.insert(idObj);
        try {
            collection.update(idObj, json("$pushAll: {field: 'value'}"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getCode()).isEqualTo(10153);
            assertThat(e.getMessage()).contains("Modifier $pushAll allowed for arrays only");
        }

        collection.update(idObj, json("$pushAll: {field: ['value', 'value2']}"));
        assertThat(collection.findOne(idObj)).isEqualTo(json("_id: 1, field: ['value', 'value2']"));
    }

    @Test
    public void testUpdateAddToSet() throws Exception {
        BasicDBObject idObj = json("_id: 1");
        collection.insert(idObj);
        collection.update(idObj, json("$addToSet: {'field.subfield.subsubfield': 'value'}"));
        assertThat(collection.findOne(idObj)).isEqualTo(json("_id: 1, field:{subfield:{subsubfield:['value']}}"));

        // addToSet to non-array
        collection.update(idObj, json("$set: {field: 'value'}"));
        try {
            collection.update(idObj, json("$addToSet: {field: 'value'}"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getCode()).isEqualTo(10141);
            assertThat(e.getMessage()).contains("Cannot apply $addToSet modifier to non-array");
        }

        // addToSet with multiple fields

        collection.update(idObj, json("$addToSet: {field1: 'value', field2: 'value2'}"));

        assertThat(collection.findOne(idObj)).isEqualTo(
                json("_id: 1, field: 'value', field1: ['value'], field2: ['value2']"));

        // addToSet duplicate
        collection.update(idObj, json("$addToSet: {field1: 'value'}"));
        assertThat(collection.findOne(idObj)).isEqualTo(
                json("_id: 1, field: 'value', field1: ['value'], field2: ['value2']"));
    }

    @Test
    public void testUpdateAddToSetEach() throws Exception {
        collection.insert(json("_id: 1"));

        collection.update(json("_id: 1"), json("$addToSet: {a: {$each: [6,5,4]}}"));
        assertThat(collection.findOne()).isEqualTo(json("_id: 1, a: [6,5,4]"));

        collection.update(json("_id: 1"), json("$addToSet: {a: {$each: [3,2,1]}}"));
        assertThat(collection.findOne()).isEqualTo(json("_id: 1, a: [6,5,4,3,2,1]"));

        collection.update(json("_id: 1"), json("$addToSet: {a: {$each: [4,7,9,2]}}"));
        assertThat(collection.findOne()).isEqualTo(json("_id: 1, a: [6,5,4,3,2,1,7,9]"));

        collection.update(json("_id: 1"), json("$addToSet: {a: {$each: [12,13,12]}}"));
        assertThat(collection.findOne()).isEqualTo(json("_id: 1, a: [6,5,4,3,2,1,7,9,12,13]"));
    }

    @Test
    public void testUpdateDatasize() throws Exception {
        DBObject obj = json("{_id:1, a:{x:[1, 2, 3]}}");
        collection.insert(obj);
        int oldSize = collection.getStats().getInt("size");

        collection.update(json("_id:1"), json("$set:{'a.x.0': 3}"));
        assertThat(collection.findOne().get("a")).isEqualTo(json("x:[3,2,3]"));
        assertThat(collection.getStats().getInt("size")).isEqualTo(oldSize);

        // now increase the db
        collection.update(json("_id:1"), json("$set:{'a.x.0': 'abc'}"));
        assertThat(collection.getStats().getInt("size") - oldSize).isEqualTo(4);
    }

    @Test
    public void testUpdatePull() throws Exception {
        BasicDBObject obj = json("_id: 1");
        collection.insert(obj);

        // pull from non-existing field
        assertThat(collection.findOne(obj)).isEqualTo(obj);

        // pull from non-array
        collection.update(obj, json("$set: {field: 'value'}"));
        try {
            collection.update(obj, json("$pull: {field: 'value'}"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getCode()).isEqualTo(10142);
            assertThat(e.getMessage()).contains("Cannot apply $pull modifier to non-array");
        }

        // pull standard
        collection.update(obj, json("$set: {field: ['value1', 'value2', 'value1']}"));

        collection.update(obj, json("$pull: {field: 'value1'}"));

        assertThat(collection.findOne(obj).get("field")).isEqualTo(Arrays.asList("value2"));

        // pull with multiple fields

        collection.update(obj, json("$set: {field1: ['value1', 'value2', 'value1']}"));
        collection.update(obj, json("$set: {field2: ['value3', 'value3', 'value1']}"));

        collection.update(obj, json("$pull: {field1: 'value2', field2: 'value3'}"));

        assertThat(collection.findOne(obj).get("field1")).isEqualTo(Arrays.asList("value1", "value1"));
        assertThat(collection.findOne(obj).get("field2")).isEqualTo(Arrays.asList("value1"));
    }

    @Test
    public void testUpdatePullAll() throws Exception {
        DBObject obj = json("_id: 1");
        collection.insert(obj);
        collection.update(obj, json("$set: {field: 'value'}"));
        try {
            collection.update(obj, json("$pullAll: {field: 'value'}"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getCode()).isEqualTo(10142);
            assertThat(e.getMessage()).contains("Cannot apply $pullAll modifier to non-array");
        }

        collection.update(obj, json("$set: {field1: ['value1', 'value2', 'value1', 'value3', 'value4', 'value3']}"));

        collection.update(obj, json("$pullAll: {field1: ['value1', 'value3']}"));

        assertThat(collection.findOne(obj).get("field1")).isEqualTo(Arrays.asList("value2", "value4"));

        try {
            collection.update(obj, json("$pullAll: {field1: 'bar'}"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getCode()).isEqualTo(10153);
            assertThat(e.getMessage()).contains("Modifier $pullAll allowed for arrays only");
        }

    }

    @Test
    public void testUpdateSet() throws Exception {
        BasicDBObject object = json("_id: 1");

        collection.insert(object);
        assertThat(collection.findOne(object)).isEqualTo(object);

        collection.update(object, json("$set: {foo: 'bar'}"));

        BasicDBObject expected = json("{}");
        expected.putAll((BSONObject) object);
        expected.put("foo", "bar");

        collection.update(object, json("$set: {bar: 'bla'}"));
        expected.put("bar", "bla");
        assertThat(collection.findOne(object)).isEqualTo(expected);

        collection.update(object, json("$set: {'foo.bar': 'bla'}"));
        expected.put("foo", json("bar: 'bla'"));
        assertThat(collection.findOne(object)).isEqualTo(expected);

        collection.update(object, json("$set: {'foo.foo': '123'}"));
        ((BasicBSONObject) expected.get("foo")).put("foo", "123");
        assertThat(collection.findOne(object)).isEqualTo(expected);
    }

    @Test
    public void testUpdateSetOnInsert() throws Exception {
        BasicDBObject object = json("_id: 1");
        collection.update(object, json("$set: {b: 3}, $setOnInsert: {a: 3}"), true, true);
        assertThat(collection.findOne()).isEqualTo(json("_id: 1, b: 3, a: 3"));

        collection.update(object, json("$set: {b: 4}, $setOnInsert: {a: 5}"), true, true);
        assertThat(collection.findOne()).isEqualTo(json("_id: 1, b: 4, a: 3")); // 'a'
                                                                                // is
                                                                                // unchanged
    }

    @Test
    public void testUpdateSetWithArrayIndices() throws Exception {

        // SERVER-181

        collection.insert(json("_id: 1, a: [{x:0}]"));
        collection.update(json("{}"), json("$set: {'a.0.x': 3}"));
        assertThat(collection.findOne()).isEqualTo(json("_id: 1, a: [{x:3}]"));

        collection.update(json("{}"), json("$set: {'a.1.z': 17}"));
        assertThat(collection.findOne()).isEqualTo(json("_id: 1, a: [{x:3}, {z:17}]"));

        collection.update(json("{}"), json("$set: {'a.0.y': 7}"));
        assertThat(collection.findOne()).isEqualTo(json("_id: 1, a: [{x:3, y:7}, {z:17}]"));

        collection.update(json("{}"), json("$set: {'a.1': 'test'}"));
        assertThat(collection.findOne()).isEqualTo(json("_id: 1, a: [{x:3, y:7}, 'test']"));
    }

    @Test
    public void testUpdateUnsetWithArrayIndices() throws Exception {

        // SERVER-273

        collection.insert(json("_id: 1, a:[{x:0}]"));
        collection.update(json("{}"), json("$unset: {'a.0.x': 1}"));
        assertThat(collection.findOne()).isEqualTo(json("_id: 1, a:[{}]"));

        collection.update(json("{}"), json("$unset: {'a.0': 1}"));
        assertThat(collection.findOne()).isEqualTo(json("_id: 1, a:[null]"));

        collection.update(json("{}"), json("$unset: {'a.10': 1}"));
        assertThat(collection.findOne()).isEqualTo(json("_id: 1, a:[null]"));
    }

    @Test
    public void testUpdatePop() throws Exception {
        BasicDBObject object = json("_id: 1");

        collection.insert(object);
        collection.update(object, json("$pop: {'foo.bar': 1}"));

        assertThat(collection.findOne(object)).isEqualTo(object);
        collection.update(object, json("$set: {'foo.bar': [1,2,3]}"));
        assertThat(collection.findOne(object)).isEqualTo(json("_id:1, foo:{bar:[1,2,3]}"));

        collection.update(object, json("$pop: {'foo.bar': 1}"));
        assertThat(collection.findOne(object)).isEqualTo(json("_id:1, foo:{bar:[1,2]}"));

        collection.update(object, json("$pop: {'foo.bar': -1}"));
        assertThat(collection.findOne(object)).isEqualTo(json("_id:1, foo:{bar:[2]}"));

        collection.update(object, json("$pop: {'foo.bar': null}"));
        assertThat(collection.findOne(object)).isEqualTo(json("_id:1, foo:{bar:[]}"));

    }

    @Test
    public void testUpdateUnset() throws Exception {
        DBObject obj = json("_id: 1, a: 1, b: null, c: 'value'");
        collection.insert(obj);
        try {
            collection.update(obj, json("$unset: {_id: ''}"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getCode()).isEqualTo(10148);
            assertThat(e.getMessage()).contains("Mod on _id not allowed");
        }

        collection.update(obj, json("$unset: {a:'', b:''}"));
        assertThat(collection.findOne()).isEqualTo(json("_id: 1, c: 'value'"));

        collection.update(obj, json("$unset: {'c.y': 1}"));
        assertThat(collection.findOne()).isEqualTo(json("_id: 1, c: 'value'"));

        collection.update(json("_id: 1"), json("a: {b: 'foo', c: 'bar'}"));

        collection.update(json("_id: 1"), json("$unset: {'a.b':1}"));
        assertThat(collection.findOne()).isEqualTo(json("_id: 1, a: {c: 'bar'}"));
    }

    @Test
    public void testUpdateWithIdIn() {
        collection.insert(json("_id: 1"));
        DBObject update = json("$push: {n: {_id: 2, u:3}}, $inc: {c:4}");
        DBObject expected = json("_id: 1, n: [{_id: 2, u:3}], c:4");
        collection.update(json("_id: {$in: [1]}"), update, false, true);
        assertThat(collection.findOne()).isEqualTo(expected);
    }

    @Test
    public void testUpdateMulti() throws Exception {
        collection.insert(json("a: 1"));
        collection.insert(json("a: 1"));
        WriteResult result = collection.update(json("a: 1"), json("$set: {b: 2}"));

        assertThat(result.getN()).isEqualTo(1);
        assertThat(result.getField("updatedExisting")).isEqualTo(Boolean.TRUE);

        assertThat(collection.find(new BasicDBObject("b", 2)).count()).isEqualTo(1);

        result = collection.update(json("a: 1"), json("$set: {b: 3}"), false, true);
        assertThat(result.getN()).isEqualTo(2);
        assertThat(result.getField("updatedExisting")).isEqualTo(Boolean.TRUE);
        assertThat(collection.find(new BasicDBObject("b", 2)).count()).isEqualTo(0);
        assertThat(collection.find(new BasicDBObject("b", 3)).count()).isEqualTo(2);
    }

    @Test
    public void testUpdateIllegalInt() throws Exception {
        collection.insert(json("_id: 1, a:{x:1}"));
        try {
            collection.update(json("_id: 1"), json("$inc: {a: 1}"));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getMessage()).contains("can not increment");
        }
    }

    @Test
    public void testUpdateWithIdInMulti() {
        collection.insert(json("_id: 1"), json("_id: 2"));
        collection.update(json("_id: {$in:[1,2]}"), json("$set: {n:1}"), false, true);
        List<DBObject> results = collection.find().toArray();
        assertThat(results).containsExactly(json("_id: 1, n:1"), json("_id: 2, n: 1"));
    }

    @Test
    public void testUpdateWithIdInMultiReturnModifiedDocumentCount() {
        collection.insert(json("_id: 1"), json("_id: 2"));
        WriteResult result = collection.update(json("_id: {$in:[1,2]}"), json("$set:{n:1}"), false, true);
        assertThat(result.getN()).isEqualTo(2);
    }

    @Test
    public void testUpdateWithIdQuery() {
        collection.insert(json("_id: 1"), json("_id: 2"));
        collection.update(json("_id: {$gt:1}"), json("$set: {n:1}"), false, true);
        List<DBObject> results = collection.find().toArray();
        assertThat(results).containsExactly(json("_id: 1"), json("_id: 2, n:1"));
    }

    @Test
    public void testUpdateWithObjectId() {
        collection.insert(json("_id: {n:1}"));
        WriteResult result = collection.update(json("_id: {n:1}"), json("$set: {a:1}"), false, false);
        assertThat(collection.findOne()).isEqualTo(json("_id: {n:1}, a:1"));
        assertThat(result.getN()).isEqualTo(1);
    }

    @Test
    public void testUpdateArrayMatch() throws Exception {

        collection.insert(json("_id:1, a:[{x:1,y:1}, {x:2,y:2}, {x:3,y:3}]"));

        collection.update(json("'a.x': 2"), json("$inc: {'a.$.y': 1}"));

        assertThat(collection.findOne(json("'a.x': 2"))).isEqualTo(json("_id:1, a:[{x:1,y:1}, {x:2,y:3}, {x:3,y:3}]"));

        collection.insert(json("{'array': [{'123a':{'name': 'old'}}]}"));
        assertThat(collection.findOne(json("{'array.123a.name': 'old'}"))).isNotNull();
        collection.update(json("{'array.123a.name': 'old'}"), json("{$set: {'array.$.123a.name': 'new'}}"));
        assertThat(collection.findOne(json("{'array.123a.name': 'new'}"))).isNotNull();
        assertThat(collection.findOne(json("{'array.123a.name': 'old'}"))).isNull();
    }

    @Test
    public void testMultiUpdateArrayMatch() throws Exception {
        collection.insert(json("{}"));
        collection.insert(json("x:[1,2,3]"));
        collection.insert(json("x:99"));

        collection.update(json("x:2"), json("$inc:{'x.$': 1}"), false, true);
        assertThat(collection.findOne(json("x:1")).get("x")).isEqualTo(Arrays.asList(1, 3, 3));
    }

    @Test
    public void testMultiUpdateNoOperator() throws Exception {
        collection.insert(json("x:99"));
        try {
            collection.update(json("{x:99}"), json("x:99, y:17"), false, true);
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getCode()).isEqualTo(10158);
            assertThat(e.getMessage()).contains("multi update only works with $ operators");
        }
    }

    @Test
    public void testUpsert() {
        WriteResult result = collection.update(json("n:'jon'"), json("$inc:{a:1}"), true, false);
        assertThat(result.getN()).isEqualTo(1);
        assertThat(result.getField("updatedExisting")).isEqualTo(Boolean.FALSE);

        DBObject object = collection.findOne();
        assertThat(result.getField("upserted")).isEqualTo(object.get("_id"));

        object.removeField("_id");
        assertThat(object).isEqualTo(json("n:'jon', a:1"));

        result = collection.update(json("_id: 17, n:'jon'"), json("$inc:{a:1}"), true, false);
        assertThat(result.getField("updatedExisting")).isEqualTo(Boolean.FALSE);
        assertThat(result.getField("upserted")).isNull();
        assertThat(collection.findOne(json("_id:17"))).isEqualTo(json("_id: 17, n:'jon', a:1"));
    }

    @Test
    public void testUpsertFieldOrder() throws Exception {
        collection.update(json("'x.y': 2"), json("$inc: {a:7}"), true, false);
        DBObject obj = collection.findOne();
        obj.removeField("_id");
        // this actually differs from the official MongoDB implementation
        assertThat(obj).isEqualTo(json("x:{y:2}, a:7"));
    }

    @Test
    public void testUpsertWithoutId() {
        WriteResult result = collection.update(json("a:1"), json("a:2"), true, false);
        assertThat(result.getN()).isEqualTo(1);
        assertThat(result.getField("updatedExisting")).isEqualTo(Boolean.FALSE);
        assertThat(collection.findOne().get("_id")).isInstanceOf(ObjectId.class);
        assertThat(collection.findOne().get("a")).isEqualTo(2);
    }

    @Test
    public void testUpsertOnIdWithPush() {
        DBObject update1 = json("$push: {c: {a:1, b:2} }");
        DBObject update2 = json("$push: {c: {a:3, b:4} }");

        collection.update(json("_id: 1"), update1, true, false);

        collection.update(json("_id: 1"), update2, true, false);

        DBObject expected = json("_id: 1, c: [{a:1, b:2}, {a:3, b:4}]");

        assertThat(collection.findOne(json("'c.a':3, 'c.b':4"))).isEqualTo(expected);
    }

    @Test
    public void testUpsertWithConditional() {
        DBObject query = json("_id: 1, b: {$gt: 5}");
        BasicDBObject update = json("$inc: {a: 1}");
        collection.update(query, update, true, false);
        assertThat(collection.findOne()).isEqualTo(json("_id: 1, a: 1"));
    }

    @Test
    public void testUpsertWithEmbeddedQuery() {
        collection.update(json("_id: 1, e.i: 1"), json("$set: {a:1}"), true, false);
        assertThat(collection.findOne(json("_id: 1"))).isEqualTo(json("_id:1, e: {i:1}, a:1"));
    }

    @Test
    public void testUpsertWithIdIn() throws Exception {
        DBObject query = json("_id: {$in: [1]}");
        DBObject update = json("$push: {n: {_id: 2 ,u : 3}}, $inc: {c: 4}");
        DBObject expected = json("_id: 1, n: [{_id: 2 ,u : 3}], c: 4");

        collection.update(query, update, true, false);

        // the ID generation actually differs from official MongoDB which just
        // create a random object id
        DBObject actual = collection.findOne();
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testIsMaster() throws Exception {
        CommandResult isMaster = db.command("isMaster");
        assertThat(isMaster.ok()).isTrue();
        assertThat(isMaster.getBoolean("ismaster")).isTrue();
        assertThat(isMaster.getDate("localTime")).isInstanceOf(Date.class);
        assertThat(isMaster.getInt("maxBsonObjectSize")).isGreaterThan(1000);
        assertThat(isMaster.getInt("maxMessageSizeBytes")).isGreaterThan(isMaster.getInt("maxBsonObjectSize"));
    }

    // https://github.com/foursquare/fongo/pull/26
    // http://stackoverflow.com/questions/12403240/storing-null-vs-not-storing-the-key-at-all-in-mongodb
    @Test
    public void testFindWithNullOrNoFieldFilter() {

        collection.insert(json("name: 'jon', group: 'group1'"));
        collection.insert(json("name: 'leo', group: 'group1'"));
        collection.insert(json("name: 'neil1', group: 'group2'"));
        collection.insert(json("name: 'neil2', group: null"));
        collection.insert(json("name: 'neil3'"));

        // check {group: null} vs {group: {$exists: false}} filter
        List<DBObject> objs = collection.find(json("group: null")).toArray();
        assertThat(objs).as("should have two neils (neil2, neil3)").hasSize(2);

        objs = collection.find(json("group: {$exists: false}")).toArray();
        assertThat(objs).as("should have one neils (neil3)").hasSize(1);

        // same check but for fields which do not exist in DB
        objs = collection.find(json("other: null")).toArray();
        assertThat(objs).as("should return all documents").hasSize(5);

        objs = collection.find(json("other: {$exists: false}")).toArray();
        assertThat(objs).as("should return all documents").hasSize(5);
    }

    // https://github.com/foursquare/fongo/issues/28
    @Test
    public void testExplicitlyAddedObjectIdNotNew() {
        ObjectId oid = new ObjectId();
        assertThat(oid.isNew()).as("should be new").isTrue();
        collection.save(new BasicDBObject("_id", oid));
        ObjectId retrievedOid = (ObjectId) collection.findOne().get("_id");
        assertThat(retrievedOid).as("retrieved should still equal the inserted").isEqualTo(oid);
        assertThat(retrievedOid.isNew()).as("retrieved should not be new").isFalse();
    }

    // https://github.com/foursquare/fongo/issues/28
    @Test
    public void testAutoCreatedObjectIdNotNew() {
        collection.save(new BasicDBObject());
        ObjectId retrievedOid = (ObjectId) collection.findOne().get("_id");
        assertThat(retrievedOid.isNew()).as("retrieved should not be new").isFalse();
    }

}
