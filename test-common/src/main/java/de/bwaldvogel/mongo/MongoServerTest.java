package de.bwaldvogel.mongo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.bson.Document;
import org.junit.Test;

import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;

public abstract class MongoServerTest {

    protected abstract MongoBackend createBackend() throws Exception;

    @Test(timeout = 10000)
    public void testStopListening() throws Exception {
        MongoServer server = new MongoServer(createBackend());
        MongoClient client = null;
        try {
            InetSocketAddress serverAddress = server.bind();
            client = new MongoClient(new ServerAddress(serverAddress));
            // request something
            pingServer(client);

            server.stopListenting();

            // existing clients must still work
            pingServer(client);

            // new clients must fail
            client.close();
            try (Socket socket = new Socket()) {
                socket.connect(serverAddress);
                fail("IOException expected");
            } catch (IOException e) {
                // expected
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
        InetSocketAddress serverAddress = server.bind();
        MongoClient client = new MongoClient(new ServerAddress(serverAddress));

        // request something to open a connection
        pingServer(client);

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
        try {
            InetSocketAddress serverAddress = server.bind();
            try (MongoClient client = new MongoClient(new ServerAddress(serverAddress))) {
                // request something to open a connection
                pingServer(client);

                server.shutdownNow();

                try {
                    pingServer(client);
                    fail("MongoException expected");
                } catch (MongoException e) {
                    // okay
                }

                // restart
                server.bind(serverAddress);

                pingServer(client);
            }
        } finally {
            server.shutdownNow();
        }
    }

    private void pingServer(MongoClient client) {
        client.getDatabase("admin").runCommand(new Document("ping", 1));
    }

}
