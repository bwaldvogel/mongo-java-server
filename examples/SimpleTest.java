import static org.junit.Assert.assertEquals;

import java.net.InetSocketAddress;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

import de.bwaldvogel.mongo.backend.memory.MemoryBackend;
import de.bwaldvogel.mongo.wire.message.MongoServer;

public class SimpleTest {

    @org.junit.Test
    public void testSimple() throws Exception {
        MongoServer server = new MongoServer( new MemoryBackend() );

        // bind on a random local port
        InetSocketAddress serverAddress = server.bind();

        MongoClient client = new MongoClient( new ServerAddress( serverAddress ) );

        DBCollection collection = client.getDB( "testdb" ).getCollection( "testcollection" );

        assertEquals( 0, collection.count() );

        // creates the database and collection in memory and insert the object
        collection.insert( new BasicDBObject( "key" , "value" ) );

        assertEquals( 1, collection.count() );

        assertEquals( "value", collection.find().toArray().get( 0 ).get( "key" ) );

        client.close();
        server.shutdown();
    }

}
