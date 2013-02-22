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

import org.bson.BSONObject;
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
import com.mongodb.ServerAddress;

import de.bwaldvogel.mongo.backend.memory.MemoryBackend;
import de.bwaldvogel.mongo.wire.message.MongoServer;

public class MemoryBackendTest {
    private Mongo client;
    private MongoServer mongoServer;
    private DB db;
    private DB admin;

    @Before
    public void setUp() throws Exception {
        mongoServer = new MongoServer(new MemoryBackend());
        InetSocketAddress serverAddress = mongoServer.bind();
        client = new MongoClient(new ServerAddress(serverAddress));
        db = client.getDB("testdb");
        admin = client.getDB("admin");
    }

    @After
    public void tearDown() {
        client.close();
        mongoServer.shutdownNow();
    }

    private CommandResult command(String command) {
        return admin.command(command);
    }

    @Test
    public void testMaxBsonSize() throws Exception {
        int maxBsonObjectSize = client.getMaxBsonObjectSize();
        assertThat(maxBsonObjectSize).isEqualTo(16777216);
    }

    @Test
    public void testServerStatus() throws Exception {
        Date before = new Date();
        CommandResult serverStatus = command("serverStatus");
        serverStatus.throwOnError();
        assertThat(serverStatus.get("uptime")).isInstanceOf(Integer.class);
        assertThat(serverStatus.get("uptimeMillis")).isInstanceOf(Long.class);
        Date serverTime = (Date) serverStatus.get("localTime");
        assertThat(serverTime).isNotNull();
        assertThat(serverTime.after(new Date())).isFalse();
        assertThat(before.after(serverTime)).isFalse();

        BSONObject connections = (BSONObject) serverStatus.get("connections");
        assertThat(connections.get("current")).isEqualTo(Integer.valueOf(1));
        System.out.println(connections);
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
        assertThat(((Number) stats.get("objects")).longValue()).isEqualTo(0);
        assertThat(((Number) stats.get("collections")).longValue()).isEqualTo(1);
        assertThat(((Number) stats.get("indexes")).longValue()).isEqualTo(1);
        assertThat(((Number) stats.get("dataSize")).longValue()).isEqualTo(0);

        db.getCollection("foo").insert(new BasicDBObject());
        db.getCollection("foo").insert(new BasicDBObject());
        db.getCollection("bar").insert(new BasicDBObject());

        stats = db.getStats();
        stats.throwOnError();

        assertThat(((Number) stats.get("objects")).longValue()).isEqualTo(5);
        assertThat(((Number) stats.get("collections")).longValue()).isEqualTo(3);
        assertThat(((Number) stats.get("indexes")).longValue()).isEqualTo(3);
        assertThat(((Number) stats.get("dataSize")).longValue()).isEqualTo(118);
    }

    @Test
    public void testCollectionStats() throws Exception {
        DBCollection collection = db.getCollection("test");
        CommandResult stats = collection.getStats();
        stats.throwOnError();
        assertThat(((Number) stats.get("count")).longValue()).isEqualTo(0);
        assertThat(((Number) stats.get("size")).longValue()).isEqualTo(0);

        collection.insert(new BasicDBObject());
        collection.insert(new BasicDBObject("abc", "foo"));
        stats = collection.getStats();
        stats.throwOnError();
        assertThat(((Number) stats.get("count")).longValue()).isEqualTo(2);
        assertThat(((Number) stats.get("size")).longValue()).isEqualTo(57);
        assertThat(((Number) stats.get("avgObjSize")).doubleValue()).isEqualTo(28.5);
    }

    @Test
    public void testListDatabaseNames() throws Exception {
        assertThat(client.getDatabaseNames()).isEmpty();
        db.getCollection("testcollection").insert(new BasicDBObject());
        assertThat(client.getDatabaseNames()).containsExactly("testdb");
        client.getDB("bar").getCollection("testcollection").insert(new BasicDBObject());
        assertThat(client.getDatabaseNames()).containsExactly("bar", "testdb");
    }

    @Test
    public void testReservedCollectionNames() throws Exception {
        try {
            db.getCollection("foo$bar").insert(new BasicDBObject());
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getMessage()).contains("illegal collection name");
        }

        try {
            db.getCollection("").insert(new BasicDBObject());
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getMessage().toLowerCase()).contains("invalid ns");
        }

        try {
            db.getCollection("system.bla").insert(new BasicDBObject());
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getMessage()).contains("illegal collection name");
        }

        try {
            db.getCollection(
                    "verylongstringverylongstringverylongstringverylongstringverylongstringverylongstringverylongstringverylongstringverylongstringverylongstringverylongstringverylongstringverylongstringverylongstringverylongstringverylongstring")
                    .insert(new BasicDBObject());
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getMessage()).contains("name too long");
        }
    }

    @Test
    public void testIllegalCommand() throws Exception {
        try {
            command("foo").throwOnError();
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getMessage()).contains("no such cmd");
        }

        try {
            client.getDB("bar").command("foo").throwOnError();
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getMessage()).contains("no such cmd");
        }
    }

    @Test
    public void testQuery() throws Exception {
        DBCollection collection = db.getCollection("testcollection");
        DBObject obj = collection.findOne(new BasicDBObject("_id", 1));
        assertThat(obj).isNull();
        assertThat(collection.count()).isEqualTo(0);
    }

    @Test
    public void testQueryCount() throws Exception {
        DBCollection collection = db.getCollection("testcollection");

        for (int i = 0; i < 100; i++) {
            collection.insert(new BasicDBObject());
        }
        assertThat(collection.count()).isEqualTo(100);

        BasicDBObject obj = new BasicDBObject("_id", 1);
        assertThat(collection.find(obj).count()).isEqualTo(0);
        collection.insert(obj);
        assertThat(collection.find(obj).count()).isEqualTo(1);
    }

    @Test
    public void testQueryAll() throws Exception {
        DBCollection collection = db.getCollection("testcollection");

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
    @SuppressWarnings("resource")
    public void testQuerySkipLimit() throws Exception {
        DBCollection collection = db.getCollection("testcollection");
        for (int i = 0; i < 10; i++) {
            collection.insert(new BasicDBObject());
        }

        DBCursor cursor = collection.find().skip(3);
        assertThat(cursor.itcount()).isEqualTo(7);

        cursor = collection.find().skip(3).limit(5);
        assertThat(cursor.itcount()).isEqualTo(5);
    }

    @Test
    public void testQuerySort() throws Exception {
        DBCollection collection = db.getCollection("testcollection");
        Random random = new Random(4711);
        for (int i = 0; i < 10; i++) {
            collection.insert(new BasicDBObject("_id", Double.valueOf(random.nextDouble())));
        }

        List<DBObject> objects = collection.find().sort(new BasicDBObject("_id", Integer.valueOf(1))).toArray();
        double before = Double.MIN_VALUE;
        for (DBObject dbObject : objects) {
            double value = ((Number) dbObject.get("_id")).doubleValue();
            assertThat(value).isGreaterThanOrEqualTo(before);
            before = value;
        }

        // reverse sort
        objects = collection.find().sort(new BasicDBObject("_id", Integer.valueOf(-1))).toArray();
        before = Double.MAX_VALUE;
        for (DBObject dbObject : objects) {
            double value = ((Number) dbObject.get("_id")).doubleValue();
            assertThat(value).isLessThanOrEqualTo(before);
            before = value;
        }
    }

    @Test
    public void testQueryLimit() throws Exception {
        DBCollection collection = db.getCollection("testcollection");
        for (int i = 0; i < 5; i++) {
            collection.insert(new BasicDBObject());
        }
        List<DBObject> objects = collection.find().limit(1).toArray();
        assertThat(objects.size()).isEqualTo(1);
    }

    @Test
    public void testInsert() throws Exception {
        DBCollection collection = db.getCollection("testcollection");
        assertThat(collection.count()).isEqualTo(0);

        for (int i = 0; i < 3; i++) {
            collection.insert(new BasicDBObject("_id", Integer.valueOf(i)));
        }

        assertThat(collection.count()).isEqualTo(3);

        collection.insert(new BasicDBObject("foo", Arrays.asList(1, 2, 3)));
        collection.insert(new BasicDBObject("foo", new byte[10]));
        BasicDBObject insertedObject = new BasicDBObject("foo", UUID.randomUUID());
        collection.insert(insertedObject);
        assertThat(collection.findOne(insertedObject)).isEqualTo(insertedObject);
    }

    @Test
    public void testInsertDuplicate() throws Exception {
        DBCollection collection = db.getCollection("testcollection");
        assertThat(collection.count()).isEqualTo(0);

        collection.insert(new BasicDBObject("_id", 1));
        assertThat(collection.count()).isEqualTo(1);

        try {
            collection.insert(new BasicDBObject("_id", 1));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getMessage()).contains("duplicate key error");
        }

        try {
            collection.insert(new BasicDBObject("_id", 1.0));
            fail("MongoException expected");
        } catch (MongoException e) {
            assertThat(e.getMessage()).contains("duplicate key error");
        }

        assertThat(collection.count()).isEqualTo(1);
    }

    @Test
    public void testQueryNull() throws Exception {
        DBCollection collection = db.getCollection("testcollection");
        BasicDBObject object = new BasicDBObject("_id", 1);
        collection.insert(object);
        assertThat(collection.findOne(new BasicDBObject("foo", null))).isEqualTo(object);
    }

    @Test
    public void testInsertQuery() throws Exception {
        DBCollection collection = db.getCollection("testcollection");
        assertThat(collection.count()).isEqualTo(0);

        BasicDBObject insertedObject = new BasicDBObject("_id", 1);
        insertedObject.put("foo", "bar");
        collection.insert(insertedObject);

        assertThat(collection.findOne(insertedObject)).isEqualTo(insertedObject);
        assertThat(collection.findOne(new BasicDBObject("_id", 1l))).isEqualTo(insertedObject);
        assertThat(collection.findOne(new BasicDBObject("_id", 1.0))).isEqualTo(insertedObject);
        assertThat(collection.findOne(new BasicDBObject("_id", 1.0001))).isNull();
        assertThat(collection.findOne(new BasicDBObject("foo", "bar"))).isEqualTo(insertedObject);
    }

    @Test
    public void testInsertRemove() throws Exception {
        DBCollection collection = db.getCollection("testcollection");

        for (int i = 0; i < 10; i++) {
            collection.insert(new BasicDBObject("_id", 1));
            assertThat(collection.count()).isEqualTo(1);
            collection.remove(new BasicDBObject("_id", 1));
            assertThat(collection.count()).isZero();

            collection.insert(new BasicDBObject("_id", i));
            collection.remove(new BasicDBObject("_id", i));
        }
        assertThat(collection.count()).isZero();
        collection.remove(new BasicDBObject("doesnt exist", 1));
        assertThat(collection.count()).isZero();
    }

    @Test
    public void testUpdate() throws Exception {
        DBCollection collection = db.getCollection("testcollection");

        BasicDBObject object = new BasicDBObject("_id", 1);

        BasicDBObject newObject = new BasicDBObject("_id", 1);
        newObject.put("foo", "bar");

        collection.insert(object);
        collection.update(object, newObject);
        assertThat(collection.findOne(object)).isEqualTo(newObject);
    }

    @Test
    public void testUpdateSet() throws Exception {
        DBCollection collection = db.getCollection("testcollection");

        BasicDBObject object = new BasicDBObject("_id", 1);

        collection.insert(object);
        assertThat(collection.findOne(object)).isEqualTo(object);

        collection.update(object, new BasicDBObject("$set", new BasicDBObject("foo", "bar")));

        BasicDBObject expected = new BasicDBObject();
        expected.putAll((BSONObject) object);
        expected.put("foo", "bar");

        collection.update(object, new BasicDBObject("$set", new BasicDBObject("bar", "bla")));
        expected.put("bar", "bla");
        assertThat(collection.findOne(object)).isEqualTo(expected);
    }

    @Test
    public void testUpsert() throws Exception {
        DBCollection collection = db.getCollection("testcollection");

        BasicDBObject object = new BasicDBObject("_id", 1);

        BasicDBObject newObject = new BasicDBObject("_id", 1);
        newObject.put("foo", "bar");

        collection.update(object, newObject, true, false);
        assertThat(collection.findOne(object)).isEqualTo(newObject);
    }

    @Test
    public void testDropDatabase() throws Exception {
        db.getCollection("foo").insert(new BasicDBObject());
        assertThat(client.getDatabaseNames()).containsExactly("testdb");
        client.dropDatabase("testdb");
        assertThat(client.getDatabaseNames()).isEmpty();
    }

    @Test
    public void testDropCollection() throws Exception {
        db.getCollection("foo").insert(new BasicDBObject());
        assertThat(db.getCollectionNames()).containsOnly("foo");
        db.getCollection("foo").drop();
        assertThat(db.getCollectionNames()).isEmpty();
    }

    @Test
    public void testReplicaSetInfo() throws Exception {
        // ReplicaSetStatus status = mongo.getReplicaSetStatus();
        // System.out.println( status );
        // assertThat( status );
    }
}
