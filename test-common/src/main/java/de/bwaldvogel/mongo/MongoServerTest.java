package de.bwaldvogel.mongo;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.junit.Test;

import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;

public abstract class MongoServerTest {

    protected abstract MongoBackend createBackend() throws Exception;

    @Test(timeout = 10000)
    public void testStopListenting() throws Exception {
        MongoServer server = new MongoServer(createBackend());
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
            Socket socket = new Socket();
            try {
                socket.connect(serverAddress);
                fail("IOException expected");
            } catch (IOException e) {
                // expected
            } finally {
                socket.close();
            }

        } finally {
            if (client != null) {
                client.close();
            }
            server.shutdownNow();
        }
    }

    @Test(timeout = 10000)
    public void testShutdownNow() throws Exception {
        MongoServer server = new MongoServer(createBackend());
        MongoClient client = null;
        InetSocketAddress serverAddress = server.bind();
        client = new MongoClient(new ServerAddress(serverAddress));

        // request something to open a connection
        client.getDB("admin").command("serverStatus").throwOnError();

        server.shutdownNow();
    }

    @Test(timeout = 5000)
    public void testGetLocalAddress() throws Exception {
        MongoServer server = new MongoServer(createBackend());
        assertThat(server.getLocalAddress()).isNull();
        try {
            InetSocketAddress serverAddress = server.bind();
            InetSocketAddress localAddress = server.getLocalAddress();
            assertThat(localAddress).isEqualTo(serverAddress);
        } finally {
            server.shutdownNow();
        }
        assertThat(server.getLocalAddress()).isNull();
    }

    @Test(timeout = 10000)
    public void testShutdownAndRestart() throws Exception {
        MongoServer server = new MongoServer(createBackend());
        InetSocketAddress serverAddress = server.bind();
        {
            final MongoClient client = new MongoClient(new ServerAddress(serverAddress));

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

            client.close();
        }
        {
            // Explicitly reconnect the client.
            // Fails otherwise with mongo-java-driver 2.12.0 unless we would use
            // a Thread.sleep(100) or so.
            final MongoClient client = new MongoClient(new ServerAddress(serverAddress));
            client.getDB("admin").command("serverStatus").throwOnError();
            client.close();
        }
        server.shutdownNow();
    }
}
