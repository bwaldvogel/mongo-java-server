package de.bwaldvogel.mongo.backend;

import java.net.InetSocketAddress;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.util.JSON;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.MongoServer;

public abstract class AbstractSimpleBackendTest {

    private MongoServer mongoServer;

    protected Mongo client;
    protected DB db;
    protected DBCollection collection;

    protected CommandResult command(String command) {
        return getAdminDb().command(command);
    }

    protected DBCollection getCollection(String collectionName) {
        return db.getCollection(collectionName);
    }

    protected BasicDBObject json(String string) {
        string = string.trim();
        if (!string.startsWith("{")) {
            string = "{" + string + "}";
        }
        return (BasicDBObject) JSON.parse(string);
    }

    protected DB getAdminDb() {
        return client.getDB("admin");
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
        mongoServer = new MongoServer(createBackend());
        InetSocketAddress serverAddress = mongoServer.bind();
        client = new MongoClient(new ServerAddress(serverAddress));
        db = client.getDB("testdb");
        collection = db.getCollection("testcoll");
    }

    protected void shutdownServer() {
        client.close();
        mongoServer.shutdownNow();
    }

    @Test
    public void testInsert() throws Exception {
        collection.insert(json("_id: 1"));
    }

    @Test
    public void testInsertDelete() throws Exception {
        collection.insert(json("_id: 1"));
        collection.remove(json("_id: 1"));
    }

}
