package de.bwaldvogel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

import de.bwaldvogel.mongo.backend.Constants;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;
import de.bwaldvogel.mongo.wire.message.MongoServer;

public class AdvancedMemoryBackendTest {

    private Mongo client;
    private MongoServer mongoServer;
    private DBCollection collection;
    private DB db;

    @Before
    public void setUp() throws Exception {
        mongoServer = new MongoServer(new MemoryBackend());
        InetSocketAddress serverAddress = mongoServer.bind();
        client = new MongoClient(new ServerAddress(serverAddress));
        db = client.getDB("db");
        collection = db.getCollection("coll");
    }

    @After
    public void tearDown() {
        client.close();
        mongoServer.shutdownNow();
    }

    @Test
    public void testGetDb() {
        collection.insert(new BasicDBObject());
        assertNotNull(collection.getDB());
        assertSame("getDB should be idempotent", collection.getDB(), client.getDB(db.getName()));
        assertEquals(Arrays.asList(db.getName()), client.getDatabaseNames());
    }

    @Test
    public void testGetCollection() {
        DBCollection collection = db.getCollection("coll");
        db.getCollection("coll").insert(new BasicDBObject());
        assertNotNull(collection);
        assertSame("getCollection should be idempotent", collection, db.getCollection("coll"));
        assertSame("getCollection should be idempotent", collection, db.getCollectionFromString("coll"));
        assertEquals(new HashSet<String>(Arrays.asList("coll")), db.getCollectionNames());
    }

    @Test
    public void testCountCommand() {
        assertEquals(0, collection.count());
    }

    @Test
    public void testCountWithQueryCommand() {
        collection.insert(new BasicDBObject("n", 1));
        collection.insert(new BasicDBObject("n", 2));
        collection.insert(new BasicDBObject("n", 2));
        assertEquals(2, collection.count(new BasicDBObject("n", 2)));
    }

    @Test
    public void testInsertIncrementsCount() {
        collection.insert(new BasicDBObject("name", "jon"));
        assertEquals(1, collection.count());
    }

    @Test
    public void testFindOne() {
        collection.insert(new BasicDBObject("name", "jon"));
        DBObject result = collection.findOne();
        assertNotNull(result);
        assertNotNull("should have an _id", result.get("_id"));
    }

    @Test
    public void testFindOneIn() {
        collection.insert(new BasicDBObject("_id", 1));
        DBObject result = collection.findOne(new BasicDBObject("_id", new BasicDBObject("$in", Arrays.asList(1, 2))));
        assertEquals(new BasicDBObject("_id", 1), result);
    }

    @Test
    public void testFindOneById() {
        collection.insert(new BasicDBObject("_id", 1));
        DBObject result = collection.findOne(new BasicDBObject("_id", 1));
        assertEquals(new BasicDBObject("_id", 1), result);

        assertEquals(null, collection.findOne(new BasicDBObject("_id", 2)));
    }

    @Test
    public void testFindWithQuery() {
        collection.insert(new BasicDBObject("name", "jon"));
        collection.insert(new BasicDBObject("name", "leo"));
        collection.insert(new BasicDBObject("name", "neil"));
        collection.insert(new BasicDBObject("name", "neil"));
        DBCursor cursor = collection.find(new BasicDBObject("name", "neil"));
        assertEquals("should have two neils", 2, cursor.toArray().size());
    }

    @Test
    public void testFindWithLimit() {
        collection.insert(new BasicDBObject("_id", 1));
        collection.insert(new BasicDBObject("_id", 2));
        collection.insert(new BasicDBObject("_id", 3));
        collection.insert(new BasicDBObject("_id", 4));

        DBCursor cursor = collection.find().limit(2);
        assertEquals(Arrays.asList(new BasicDBObject("_id", 1), new BasicDBObject("_id", 2)), cursor.toArray());
    }

    @Test
    public void testFindWithSkipLimit() {
        collection.insert(new BasicDBObject("_id", 1));
        collection.insert(new BasicDBObject("_id", 2));
        collection.insert(new BasicDBObject("_id", 3));
        collection.insert(new BasicDBObject("_id", 4));

        DBCursor cursor = collection.find().limit(2).skip(2);
        assertEquals(Arrays.asList(new BasicDBObject("_id", 3), new BasicDBObject("_id", 4)), cursor.toArray());
    }

    @Test
    public void testIdInQueryResultsInIndexOrder() {
        collection.insert(new BasicDBObject("_id", 4));
        collection.insert(new BasicDBObject("_id", 3));
        collection.insert(new BasicDBObject("_id", 1));
        collection.insert(new BasicDBObject("_id", 2));

        DBCursor cursor = collection.find(new BasicDBObject("_id", new BasicDBObject("$in", Arrays.asList(3, 2, 1))));
        assertEquals(
                Arrays.asList(new BasicDBObject("_id", 1), new BasicDBObject("_id", 2), new BasicDBObject("_id", 3)),
                cursor.toArray());
    }

    @Test
    public void testSort() {
        collection.insert(new BasicDBObject("a", 1).append("_id", 1));
        collection.insert(new BasicDBObject("a", 2).append("_id", 2));
        collection.insert(new BasicDBObject("_id", 5));
        collection.insert(new BasicDBObject("a", 3).append("_id", 3));
        collection.insert(new BasicDBObject("a", 4).append("_id", 4));

        DBCursor cursor = collection.find().sort(new BasicDBObject("a", -1));
        assertEquals(Arrays.asList(new BasicDBObject("a", 4).append("_id", 4),
                new BasicDBObject("a", 3).append("_id", 3), new BasicDBObject("a", 2).append("_id", 2),
                new BasicDBObject("a", 1).append("_id", 1), new BasicDBObject("_id", 5)), cursor.toArray());
    }

    @Test
    @Ignore("not yet implemented")
    public void testCompoundSort() {
        collection.insert(new BasicDBObject("a", 1).append("_id", 1));
        collection.insert(new BasicDBObject("a", 2).append("_id", 5));
        collection.insert(new BasicDBObject("a", 1).append("_id", 2));
        collection.insert(new BasicDBObject("a", 2).append("_id", 4));
        collection.insert(new BasicDBObject("a", 1).append("_id", 3));

        DBCursor cursor = collection.find().sort(new BasicDBObject("a", 1).append("_id", -1));
        assertEquals(Arrays.asList(new BasicDBObject("a", 1).append("_id", 3),
                new BasicDBObject("a", 1).append("_id", 2), new BasicDBObject("a", 1).append("_id", 1),
                new BasicDBObject("a", 2).append("_id", 5), new BasicDBObject("a", 2).append("_id", 4)),
                cursor.toArray());
    }

    @Test
    @Ignore("not yet implemented")
    public void testEmbeddedSort() {
        collection.insert(new BasicDBObject("_id", 1));
        collection.insert(new BasicDBObject("_id", 2));
        collection.insert(new BasicDBObject("_id", 3));
        collection.insert(new BasicDBObject("_id", 4).append("counts", new BasicDBObject("done", 1)));
        collection.insert(new BasicDBObject("_id", 5).append("counts", new BasicDBObject("done", 2)));

        DBCursor cursor = collection.find(new BasicDBObject("c", new BasicDBObject("$ne", true))).sort(
                new BasicDBObject("counts.done", -1));
        assertEquals(Arrays.asList(new BasicDBObject("_id", 5).append("counts", new BasicDBObject("done", 2)),
                new BasicDBObject("_id", 4).append("counts", new BasicDBObject("done", 1)),
                new BasicDBObject("_id", 1), new BasicDBObject("_id", 2), new BasicDBObject("_id", 3)),
                cursor.toArray());
    }

    @Test
    public void testBasicUpdate() {
        collection.insert(new BasicDBObject("_id", 1));
        collection.insert(new BasicDBObject("_id", 2).append("b", 5));
        collection.insert(new BasicDBObject("_id", 3));
        collection.insert(new BasicDBObject("_id", 4));

        collection.update(new BasicDBObject("_id", 2), new BasicDBObject("a", 5));

        assertEquals(new BasicDBObject("_id", 2).append("a", 5), collection.findOne(new BasicDBObject("_id", 2)));
    }

    @Test
    public void testFullUpdateWithSameId() throws Exception {
        collection.insert(new BasicDBObject("_id", 1));
        collection.insert(new BasicDBObject("_id", 2).append("b", 5));
        collection.insert(new BasicDBObject("_id", 3));
        collection.insert(new BasicDBObject("_id", 4));

        collection.update(new BasicDBObject("_id", 2).append("b", 5), new BasicDBObject("_id", 2).append("a", 5));

        assertEquals(new BasicDBObject("_id", 2).append("a", 5), collection.findOne(new BasicDBObject("_id", 2)));
    }

    @Test
    public void testUpdateIdNoChange() {
        collection.insert(new BasicDBObject("_id", 1));
        collection.update(new BasicDBObject("_id", 1), new BasicDBObject("_id", 1).append("a", 5));

        assertEquals(new BasicDBObject("_id", 1).append("a", 5), collection.findOne(new BasicDBObject("_id", 1)));

        collection.update(new BasicDBObject("_id", 1),
                new BasicDBObject("$set", new BasicDBObject("_id", 1).append("b", 3)));

        assertEquals(new BasicDBObject("_id", 1).append("a", 5).append("b", 3),
                collection.findOne(new BasicDBObject("_id", 1)));

        // test with $set

        collection.update(new BasicDBObject("_id", 1),
                new BasicDBObject("$set", new BasicDBObject("_id", 1).append("a", 7)));

        assertEquals(new BasicDBObject("_id", 1).append("a", 7).append("b", 3),
                collection.findOne(new BasicDBObject("_id", 1)));
    }

    @Test
    public void testIdNotAllowedToBeUpdated() {
        collection.insert(new BasicDBObject("_id", 1));

        try {
            collection.update(new BasicDBObject("_id", 1), new BasicDBObject("_id", 2).append("a", 5));
            fail("should throw exception");
        } catch (MongoException e) {
            assertEquals("cannot change _id of a document old:{ \"_id\" : 1} new:{ \"_id\" : 2}", e.getMessage());
        }

        // test with $set

        try {
            collection.update(new BasicDBObject("_id", 1), new BasicDBObject("$set", new BasicDBObject("_id", 2)));
            fail("should throw exception");
        } catch (MongoException e) {
            assertEquals("Mod on _id not allowed", e.getMessage());
        }
    }

    @Test
    @Ignore("not yet implemented")
    public void testUpsert() {
        collection.update(new BasicDBObject("_id", 1).append("n", "jon"), new BasicDBObject("$inc", new BasicDBObject(
                "a", 1)), true, false);
        assertEquals(new BasicDBObject("_id", 1).append("n", "jon").append("a", 1), collection.findOne());
    }

    @Test
    @Ignore("not yet implemented")
    public void testUpsertWithConditional() {
        collection.update(new BasicDBObject("_id", 1).append("b", new BasicDBObject("$gt", 5)), new BasicDBObject(
                "$inc", new BasicDBObject("a", 1)), true, false);
        assertEquals(new BasicDBObject("_id", 1).append("a", 1), collection.findOne());
    }

    @Test
    @Ignore("not yet implemented")
    public void testUpsertWithIdIn() {
        DBObject query = new BasicDBObjectBuilder().push("_id").append("$in", Arrays.asList(1)).pop().get();
        DBObject update = new BasicDBObjectBuilder().push("$push").push("n").append("_id", 2).append("u", 3).pop()
                .pop().push("$inc").append("c", 4).pop().get();
        DBObject expected = new BasicDBObjectBuilder().append("_id", 1)
                .append("n", Arrays.asList(new BasicDBObject("_id", 2).append("u", 3))).append("c", 4).get();
        collection.update(query, update, true, false);
        assertEquals(expected, collection.findOne());
    }

    @Test
    public void testUpdateWithIdIn() {
        collection.insert(new BasicDBObject("_id", 1));
        DBObject query = new BasicDBObjectBuilder().push("_id").append("$in", Arrays.asList(1)).pop().get();
        DBObject update = new BasicDBObjectBuilder().push("$push").push("n").append("_id", 2).append("u", 3).pop()
                .pop().push("$inc").append("c", 4).pop().get();
        DBObject expected = new BasicDBObjectBuilder().append("_id", 1)
                .append("n", Arrays.asList(new BasicDBObject("_id", 2).append("u", 3))).append("c", 4).get();
        collection.update(query, update, false, true);
        assertEquals(expected, collection.findOne());
    }

    @Test
    public void testUpdateWithObjectId() {
        collection.insert(new BasicDBObject("_id", new BasicDBObject("n", 1)));
        DBObject query = new BasicDBObject("_id", new BasicDBObject("n", 1));
        DBObject update = new BasicDBObject("$set", new BasicDBObject("a", 1));
        collection.update(query, update, false, false);
        assertEquals(new BasicDBObject("_id", new BasicDBObject("n", 1)).append("a", 1), collection.findOne());
    }

    @Test
    public void testUpdateWithIdInMulti() {
        collection.insert(new BasicDBObject("_id", 1), new BasicDBObject("_id", 2));
        collection.update(new BasicDBObject("_id", new BasicDBObject("$in", Arrays.asList(1, 2))), new BasicDBObject(
                "$set", new BasicDBObject("n", 1)), false, true);
        List<DBObject> results = collection.find().toArray();
        assertEquals(
                Arrays.asList(new BasicDBObject("_id", 1).append("n", 1), new BasicDBObject("_id", 2).append("n", 1)),
                results);
    }

    @Test
    @Ignore("not yet implemented")
    public void testUpdateWithIdQuery() {
        collection.insert(new BasicDBObject("_id", 1), new BasicDBObject("_id", 2));
        collection.update(new BasicDBObject("_id", new BasicDBObject("$gt", 1)), new BasicDBObject("$set",
                new BasicDBObject("n", 1)), false, true);
        List<DBObject> results = collection.find().toArray();
        assertEquals(Arrays.asList(new BasicDBObject("_id", 1), new BasicDBObject("_id", 2).append("n", 1)), results);
    }

    @Test
    @Ignore("not yet implemented")
    public void testCompoundDateIdUpserts() {
        DBObject query = new BasicDBObjectBuilder().push("_id").push("$lt").add("n", "a").add("t", 10).pop()
                .push("$gte").add("n", "a").add("t", 1).pop().pop().get();
        List<BasicDBObject> toUpsert = Arrays.asList(
                new BasicDBObject("_id", new BasicDBObject("n", "a").append("t", 1)), new BasicDBObject("_id",
                        new BasicDBObject("n", "a").append("t", 2)), new BasicDBObject("_id", new BasicDBObject("n",
                        "a").append("t", 3)), new BasicDBObject("_id", new BasicDBObject("n", "a").append("t", 11)));
        for (BasicDBObject dbo : toUpsert) {
            collection.update(dbo, ((BasicDBObject) dbo.copy()).append("foo", "bar"), true, false);
        }
        System.out.println(collection.find().toArray());
        List<DBObject> results = collection.find(query).toArray();
        assertEquals(Arrays.<DBObject> asList(
                new BasicDBObject("_id", new BasicDBObject("n", "a").append("t", 1)).append("foo", "bar"),
                new BasicDBObject("_id", new BasicDBObject("n", "a").append("t", 2)).append("foo", "bar"),
                new BasicDBObject("_id", new BasicDBObject("n", "a").append("t", 3)).append("foo", "bar")), results);
    }

    @Test
    @Ignore("not yet implemented")
    public void testAnotherUpsert() {
        BasicDBObjectBuilder queryBuilder = BasicDBObjectBuilder.start().push("_id").append("f", "ca").push("1")
                .append("l", 2).pop().push("t").append("t", 11).pop().pop();
        DBObject query = queryBuilder.get();

        DBObject update = BasicDBObjectBuilder.start().push("$inc").append("n.!", 1).append("n.a.b:false", 1).pop()
                .get();
        collection.update(query, update, true, false);

        DBObject expected = queryBuilder.push("n").append("!", 1).push("a").append("b:false", 1).pop().pop().get();
        assertEquals(expected, collection.findOne());
    }

    @Test
    @Ignore("not yet implemented")
    public void testUpsertOnIdWithPush() {
        DBObject update1 = BasicDBObjectBuilder.start().push("$push").push("c").append("a", 1).append("b", 2).pop()
                .pop().get();

        DBObject update2 = BasicDBObjectBuilder.start().push("$push").push("c").append("a", 3).append("b", 4).pop()
                .pop().get();

        collection.update(new BasicDBObject("_id", 1), update1, true, false);
        collection.update(new BasicDBObject("_id", 1), update2, true, false);

        DBObject expected = new BasicDBObject("_id", 1).append("c",
                Arrays.asList(new BasicDBObject("a", 1).append("b", 2), new BasicDBObject("a", 3).append("b", 4)));

        assertEquals(expected, collection.findOne(new BasicDBObject("c.a", 3).append("c.b", 4)));
    }

    @Test
    @Ignore("not yet implemented")
    public void testUpsertWithEmbeddedQuery() {
        DBObject update = BasicDBObjectBuilder.start().push("$set").append("a", 1).pop().get();

        collection.update(new BasicDBObject("_id", 1).append("e.i", 1), update, true, false);

        DBObject expected = BasicDBObjectBuilder.start().append("_id", 1).push("e").append("i", 1).pop().append("a", 1)
                .get();

        assertEquals(expected, collection.findOne(new BasicDBObject("_id", 1)));
    }

    @Test
    @Ignore("not yet implemented")
    public void testFindAndModifyReturnOld() {
        collection.insert(new BasicDBObject("_id", 1).append("a", 1));
        DBObject result = collection.findAndModify(new BasicDBObject("_id", 1), null, null, false, new BasicDBObject(
                "$inc", new BasicDBObject("a", 1)), false, false);

        assertEquals(new BasicDBObject("_id", 1).append("a", 1), result);
        assertEquals(new BasicDBObject("_id", 1).append("a", 2), collection.findOne());
    }

    @Test
    @Ignore("not yet implemented")
    public void testFindAndModifyReturnNew() {
        collection.insert(new BasicDBObject("_id", 1).append("a", 1));
        DBObject result = collection.findAndModify(new BasicDBObject("_id", 1), null, null, false, new BasicDBObject(
                "$inc", new BasicDBObject("a", 1)), true, false);

        assertEquals(new BasicDBObject("_id", 1).append("a", 2), result);
    }

    @Test
    @Ignore("not yet implemented")
    public void testFindAndModifyUpsert() {
        DBObject result = collection.findAndModify(new BasicDBObject("_id", 1), null, null, false, new BasicDBObject(
                "$inc", new BasicDBObject("a", 1)), true, true);

        assertEquals(new BasicDBObject("_id", 1).append("a", 1), result);
        assertEquals(new BasicDBObject("_id", 1).append("a", 1), collection.findOne());
    }

    @Test
    @Ignore("not yet implemented")
    public void testFindAndModifyUpsertReturnNewFalse() {
        DBObject result = collection.findAndModify(new BasicDBObject("_id", 1), null, null, false, new BasicDBObject(
                "$inc", new BasicDBObject("a", 1)), false, true);

        assertEquals(new BasicDBObject(), result);
        assertEquals(new BasicDBObject("_id", 1).append("a", 1), collection.findOne());
    }

    @Test
    @Ignore("not yet implemented")
    public void testFindAndRemoveFromEmbeddedList() {
        BasicDBObject obj = new BasicDBObject("_id", 1).append("a", Arrays.asList(1));
        collection.insert(obj);
        DBObject result = collection.findAndRemove(new BasicDBObject("_id", 1));
        assertEquals(obj, result);
    }

    @Test
    @Ignore("not yet implemented")
    public void testFindAndModifyRemove() {
        collection.insert(new BasicDBObject("_id", 1).append("a", 1));
        DBObject result = collection.findAndModify(new BasicDBObject("_id", 1), null, null, true, null, false, false);

        assertEquals(new BasicDBObject("_id", 1).append("a", 1), result);
        assertEquals(null, collection.findOne());
    }

    @Test
    public void testRemove() {
        collection.insert(new BasicDBObject("_id", 1));
        collection.insert(new BasicDBObject("_id", 2));
        collection.insert(new BasicDBObject("_id", 3));
        collection.insert(new BasicDBObject("_id", 4));

        collection.remove(new BasicDBObject("_id", 2));

        assertEquals(null, collection.findOne(new BasicDBObject("_id", 2)));
    }

    @Test
    @Ignore("not yet implemented")
    public void testDistinctQuery() {
        collection.insert(new BasicDBObject("n", 1).append("_id", 1));
        collection.insert(new BasicDBObject("n", 2).append("_id", 2));
        collection.insert(new BasicDBObject("n", 3).append("_id", 3));
        collection.insert(new BasicDBObject("n", 1).append("_id", 4));
        collection.insert(new BasicDBObject("n", 1).append("_id", 5));
        assertEquals(Arrays.asList(1, 2, 3), collection.distinct("n"));
    }

    @Test
    public void testGetLastError() {
        collection.insert(new BasicDBObject("_id", 1));
        CommandResult error = db.getLastError();
        assertTrue(error.ok());

        assertFalse(db.command("illegalCommand").ok());

        // getlasterror must succeed again
        assertTrue(db.getLastError().ok());
    }

    @Test
    public void testSave() {
        BasicDBObject inserted = new BasicDBObject("_id", 1);
        collection.insert(inserted);
        collection.save(inserted);
    }

    @Test(expected = MongoException.DuplicateKey.class)
    public void testInsertDuplicateWithConcernThrows() {
        collection.insert(new BasicDBObject("_id", 1));
        collection.insert(new BasicDBObject("_id", 1), WriteConcern.SAFE);
    }

    @Test(expected = MongoException.DuplicateKey.class)
    public void testInsertDuplicateThrows() {
        collection.insert(new BasicDBObject("_id", 1));
        collection.insert(new BasicDBObject("_id", 1));
    }

    @Test
    @Ignore("not yet implemented")
    public void testCreateIndexes() {
        collection.ensureIndex("n");
        collection.ensureIndex("b");
        List<DBObject> indexes = db.getCollection("system.indexes").find().toArray();
        assertEquals(
                Arrays.asList(new BasicDBObject("v", 1).append("key", new BasicDBObject("n", 1))
                        .append("ns", "db.coll").append("name", "n_1"),
                        new BasicDBObject("v", 1).append("key", new BasicDBObject("b", 1)).append("ns", "db.coll")
                                .append("name", "b_1")), indexes);
    }

    @Test
    @Ignore("not yet implemented")
    public void testSortByEmbeddedKey() {
        collection.insert(new BasicDBObject("_id", 1).append("a", new BasicDBObject("b", 1)));
        collection.insert(new BasicDBObject("_id", 2).append("a", new BasicDBObject("b", 2)));
        collection.insert(new BasicDBObject("_id", 3).append("a", new BasicDBObject("b", 3)));
        List<DBObject> results = collection.find().sort(new BasicDBObject("a.b", -1)).toArray();
        assertEquals(Arrays.asList(new BasicDBObject("_id", 3).append("a", new BasicDBObject("b", 3)),
                new BasicDBObject("_id", 2).append("a", new BasicDBObject("b", 2)),
                new BasicDBObject("_id", 1).append("a", new BasicDBObject("b", 1))), results);
    }

    @Test
    @Ignore("not yet implemented")
    public void testInsertReturnModifiedDocumentCount() {
        WriteResult result = collection.insert(new BasicDBObject("_id", new BasicDBObject("n", 1)));
        assertEquals(1, result.getN());
    }

    @Test
    @Ignore("not yet implemented")
    public void testUpdateWithIdInMultiReturnModifiedDocumentCount() {
        collection.insert(new BasicDBObject("_id", 1), new BasicDBObject("_id", 2));
        WriteResult result = collection.update(new BasicDBObject("_id", new BasicDBObject("$in", Arrays.asList(1, 2))),
                new BasicDBObject("$set", new BasicDBObject("n", 1)), false, true);
        assertEquals(2, result.getN());
    }

    @Test
    @Ignore("not yet implemented")
    public void testUpdateWithObjectIdReturnModifiedDocumentCount() {
        collection.insert(new BasicDBObject("_id", new BasicDBObject("n", 1)));
        DBObject query = new BasicDBObject("_id", new BasicDBObject("n", 1));
        DBObject update = new BasicDBObject("$set", new BasicDBObject("a", 1));
        WriteResult result = collection.update(query, update, false, false);
        assertEquals(1, result.getN());
    }

    /**
     * Test that ObjectId is getting generated even if _id is present in
     * DBObject but it's value is null
     *
     * @throws Exception
     */
    @Test
    public void testIdGenerated() throws Exception {
        DBObject toSave = new BasicDBObject();
        toSave.put("_id", null);
        toSave.put("name", "test");

        collection.save(toSave);
        DBObject result = collection.findOne(new BasicDBObject("name", "test"));
        assertNotNull("Expected _id to be generated" + result.get(Constants.ID_FIELD));
    }

    @Test
    public void testDropDatabaseAlsoDropsCollectionData() throws Exception {
        collection.insert(new BasicDBObject());
        db.dropDatabase();
        assertEquals("Collection should have no data", 0, collection.count());
    }

    @Test
    public void testDropCollectionAlsoDropsFromDB() throws Exception {
        collection.insert(new BasicDBObject());
        collection.drop();
        assertEquals("Collection should have no data", 0, collection.count());
        assertFalse("Collection shouldn't exist in DB",
                db.getCollectionNames().contains(collection.getName()));
    }

    @Test
    public void testDropDatabaseDropsAllData() throws Exception {
        collection.insert(new BasicDBObject());
        client.dropDatabase(db.getName());
        assertFalse("DB shouldn't exist", client.getDatabaseNames().contains(db.getName()));
        assertEquals("Collection should have no data", 0, collection.count());
        assertFalse("Collection shouldn't exist in DB", db.getCollectionNames().contains(collection.getName()));
    }

}
