package de.bwaldvogel;

import static org.fest.assertions.Assertions.assertThat;
import static org.fest.assertions.Fail.fail;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.bson.BSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;

import de.bwaldvogel.mongo.backend.memory.MemoryBackend;
import de.bwaldvogel.mongo.wire.message.MongoServer;

public class MemoryBackendTest {
    private Mongo mongo;
    private MongoServer mongoServer;

    @Before
    public void setUp() throws Exception {
        mongoServer = new MongoServer( new MemoryBackend() );
        InetSocketAddress serverAddress = mongoServer.bind();
        mongo = new MongoClient( new ServerAddress( serverAddress ) );
    }

    @After
    public void tearDown() {
        mongo.close();
        mongoServer.shutdown();
    }

    @Test
    public void testMaxBsonSize() throws Exception {
        int maxBsonObjectSize = mongo.getMaxBsonObjectSize();
        assertThat( maxBsonObjectSize ).isEqualTo( 16777216 );
    }

    @Test
    public void testStats() throws Exception {
        CommandResult stats = mongo.getDB( "testdb" ).getStats();
        stats.throwOnError();
        assertThat( ( (Number) stats.get( "objects" ) ).longValue() ).isEqualTo( 0 );
        assertThat( ( (Number) stats.get( "collections" ) ).longValue() ).isEqualTo( 1 );
        assertThat( ( (Number) stats.get( "indexes" ) ).longValue() ).isEqualTo( 1 );
        assertThat( ( (Number) stats.get( "dataSize" ) ).longValue() ).isEqualTo( 0 );

        mongo.getDB( "testdb" ).getCollection( "foo" ).insert( new BasicDBObject() );
        mongo.getDB( "testdb" ).getCollection( "foo" ).insert( new BasicDBObject() );
        mongo.getDB( "testdb" ).getCollection( "bar" ).insert( new BasicDBObject() );

        stats = mongo.getDB( "testdb" ).getStats();
        stats.throwOnError();

        assertThat( ( (Number) stats.get( "objects" ) ).longValue() ).isEqualTo( 5 );
        assertThat( ( (Number) stats.get( "collections" ) ).longValue() ).isEqualTo( 3 );
        assertThat( ( (Number) stats.get( "indexes" ) ).longValue() ).isEqualTo( 3 );
        assertThat( ( (Number) stats.get( "dataSize" ) ).longValue() ).isEqualTo( 118 );
    }

    @Test
    public void testListDatabaseNames() throws Exception {
        assertThat( mongo.getDatabaseNames() ).isEmpty();
        mongo.getDB( "testdb" ).getCollection( "testcollection" ).insert( new BasicDBObject() );
        assertThat( mongo.getDatabaseNames() ).containsExactly( "testdb" );
        mongo.getDB( "bar" ).getCollection( "testcollection" ).insert( new BasicDBObject() );
        assertThat( mongo.getDatabaseNames() ).containsExactly( "bar", "testdb" );
    }

    @Test
    public void testIllegalCommand() throws Exception {
        try {
            mongo.getDB( "testdb" ).command( "foo" ).throwOnError();
            fail( "MongoException expected" );
        }
        catch ( MongoException e ) {
            assertThat( e.getMessage() ).contains( "no such cmd" );
        }

        try {
            mongo.getDB( "bar" ).command( "foo" ).throwOnError();
            fail( "MongoException expected" );
        }
        catch ( MongoException e ) {
            assertThat( e.getMessage() ).contains( "no such cmd" );
        }
    }

    @Test
    public void testQuery() throws Exception {
        DBCollection collection = mongo.getDB( "testdb" ).getCollection( "testcollection" );
        DBObject obj = collection.findOne( new BasicDBObject( "_id" , 1 ) );
        assertThat( obj ).isNull();
        assertThat( collection.count() ).isEqualTo( 0 );
    }

    @Test
    public void testQueryAll() throws Exception {
        DBCollection collection = mongo.getDB( "testdb" ).getCollection( "testcollection" );

        List<Object> inserted = new ArrayList<Object>();
        for ( int i = 0; i < 10; i++ ) {
            BasicDBObject obj = new BasicDBObject( "_id" , i );
            collection.insert( obj );
            inserted.add( obj );
        }
        assertThat( collection.count() ).isEqualTo( 10 );

        assertThat( collection.find().toArray() ).isEqualTo( inserted );
    }

    @Test
    public void testInsert() throws Exception {
        DBCollection collection = mongo.getDB( "testdb" ).getCollection( "testcollection" );
        assertThat( collection.count() ).isEqualTo( 0 );

        for ( int i = 0; i < 3; i++ ) {
            collection.insert( new BasicDBObject( "_id" , Integer.valueOf( i ) ) );
        }

        assertThat( collection.count() ).isEqualTo( 3 );

        collection.insert( new BasicDBObject( "foo" , Arrays.asList( 1, 2, 3 ) ) );
        collection.insert( new BasicDBObject( "foo" , new byte[10] ) );
        BasicDBObject insertedObject = new BasicDBObject( "foo" , UUID.randomUUID() );
        collection.insert( insertedObject );
        assertThat( collection.findOne( insertedObject ) ).isEqualTo( insertedObject );
    }

    @Test
    public void testInsertDuplicate() throws Exception {
        DBCollection collection = mongo.getDB( "testdb" ).getCollection( "testcollection" );
        assertThat( collection.count() ).isEqualTo( 0 );

        collection.insert( new BasicDBObject( "_id" , 1 ) );
        assertThat( collection.count() ).isEqualTo( 1 );

        try {
            collection.insert( new BasicDBObject( "_id" , 1 ) );
            fail( "MongoException expected" );
        }
        catch ( MongoException e ) {
            assertThat( e.getMessage() ).contains( "duplicate key error" );
        }

        try {
            collection.insert( new BasicDBObject( "_id" , 1.0 ) );
            fail( "MongoException expected" );
        }
        catch ( MongoException e ) {
            assertThat( e.getMessage() ).contains( "duplicate key error" );
        }

        assertThat( collection.count() ).isEqualTo( 1 );
    }

    @Test
    public void testInsertQuery() throws Exception {
        DBCollection collection = mongo.getDB( "testdb" ).getCollection( "testcollection" );
        assertThat( collection.count() ).isEqualTo( 0 );

        BasicDBObject insertedObject = new BasicDBObject( "_id" , 1 );
        insertedObject.put( "foo", "bar" );

        collection.insert( insertedObject );

        assertThat( collection.findOne( insertedObject ) ).isEqualTo( insertedObject );
        assertThat( collection.findOne( new BasicDBObject( "_id" , 1l ) ) ).isEqualTo( insertedObject );
        assertThat( collection.findOne( new BasicDBObject( "_id" , 1.0 ) ) ).isEqualTo( insertedObject );
        assertThat( collection.findOne( new BasicDBObject( "_id" , 1.0001 ) ) ).isNull();
        assertThat( collection.findOne( new BasicDBObject( "foo" , "bar" ) ) ).isEqualTo( insertedObject );
        assertThat( collection.findOne( new BasicDBObject( "foo" , null ) ) ).isEqualTo( insertedObject );
    }

    @Test
    public void testInsertRemove() throws Exception {
        DBCollection collection = mongo.getDB( "testdb" ).getCollection( "testcollection" );

        for ( int i = 0; i < 10; i++ ) {
            collection.insert( new BasicDBObject( "_id" , 1 ) );
            collection.remove( new BasicDBObject( "_id" , 1 ) );
            collection.insert( new BasicDBObject( "_id" , i ) );
            collection.remove( new BasicDBObject( "_id" , i ) );
        }
        collection.remove( new BasicDBObject( "doesnt exist" , 1 ) );
        assertThat( collection.count() ).isEqualTo( 0 );
    }

    @Test
    public void testUpdate() throws Exception {
        DBCollection collection = mongo.getDB( "testdb" ).getCollection( "testcollection" );

        BasicDBObject object = new BasicDBObject( "_id" , 1 );

        BasicDBObject newObject = new BasicDBObject( "_id" , 1 );
        newObject.put( "foo", "bar" );

        collection.insert( object );
        collection.update( object, newObject );
        assertThat( collection.findOne( object ) ).isEqualTo( newObject );
    }

    @Test
    public void testUpdateSet() throws Exception {
        DBCollection collection = mongo.getDB( "testdb" ).getCollection( "testcollection" );

        BasicDBObject object = new BasicDBObject( "_id" , 1 );

        collection.insert( object );
        assertThat( collection.findOne( object ) ).isEqualTo( object );

        collection.update( object, new BasicDBObject( "$set" , new BasicDBObject( "foo" , "bar" ) ) );

        BasicDBObject expected = new BasicDBObject();
        expected.putAll( (BSONObject) object );
        expected.put( "foo", "bar" );

        collection.update( object, new BasicDBObject( "$set" , new BasicDBObject( "bar" , "bla" ) ) );
        expected.put( "bar", "bla" );
        assertThat( collection.findOne( object ) ).isEqualTo( expected );
    }

    @Test
    public void testUpsert() throws Exception {
        DBCollection collection = mongo.getDB( "testdb" ).getCollection( "testcollection" );

        BasicDBObject object = new BasicDBObject( "_id" , 1 );

        BasicDBObject newObject = new BasicDBObject( "_id" , 1 );
        newObject.put( "foo", "bar" );

        collection.update( object, newObject, true, false );
        assertThat( collection.findOne( object ) ).isEqualTo( newObject );
    }

    @Test
    public void testDropDatabase() throws Exception {
        mongo.getDB( "testdb" ).getCollection( "foo" ).insert( new BasicDBObject() );
        assertThat( mongo.getDatabaseNames() ).containsExactly( "testdb" );
        mongo.dropDatabase( "testdb" );
        assertThat( mongo.getDatabaseNames() ).isEmpty();
    }

    @Test
    public void testDropCollection() throws Exception {
        DB db = mongo.getDB( "testdb" );
        db.getCollection( "foo" ).insert( new BasicDBObject() );
        assertThat( db.getCollectionNames() ).containsOnly( "foo" );
        db.getCollection( "foo" ).drop();
        assertThat( db.getCollectionNames() ).isEmpty();
    }

    @Test
    public void testReplicaSetInfo() throws Exception {
        // ReplicaSetStatus status = mongo.getReplicaSetStatus();
        // System.out.println(status);
        // assertThat(status)
    }

}
