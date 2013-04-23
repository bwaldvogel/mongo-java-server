package de.bwaldvogel.mongo;

import static org.junit.Assert.fail;

import java.net.InetSocketAddress;

import org.junit.Test;

import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;

public class MongoServerTest {

    @Test(timeout = 5000)
    public void testStopListenting() {
        MongoServer server = new MongoServer();
        MongoClient client = null;
        try {
            InetSocketAddress serverAddress = server.bind();
            client = new MongoClient(new ServerAddress(serverAddress));
            // request something
            client.getDB("admin").command("serverStatus").throwOnError();

            server.stopListenting();

            // existing clients must still work
            client.getDB("admin").command("serverStatus").throwOnError();

            // new clients must fail
            client.close();
            try {
                client = new MongoClient(new ServerAddress(serverAddress));
                client.getDB("admin").command("serverStatus");
                fail("MongoException expected");
            } catch (MongoException e) {
                // expected
            }

        } finally {
            if (client != null) {
                client.close();
            }
            server.shutdownNow();
        }
    }

    @Test(timeout = 1000)
    public void testShutdownNow() {
        MongoServer server = new MongoServer();
        MongoClient client = null;
        InetSocketAddress serverAddress = server.bind();
        client = new MongoClient(new ServerAddress(serverAddress));

        // request something to open a connection
        client.getDB("admin").command("serverStatus").throwOnError();

        server.shutdownNow();
    }

    @Test(timeout = 1000)
    public void testShutdownAndRestart() {
        MongoServer server = new MongoServer();
        MongoClient client = null;
        InetSocketAddress serverAddress = server.bind();
        client = new MongoClient(new ServerAddress(serverAddress));

        // request something to open a connection
        client.getDB("admin").command("serverStatus").throwOnError();

        server.shutdownNow();

        try {
            client.getDB("admin").command("serverStatus");
            fail("MongoException expected");
        } catch (MongoException e) {
            // okay
        }

        server.bind(serverAddress);

        client.getDB("admin").command("serverStatus").throwOnError();

        server.shutdownNow();
    }
}
