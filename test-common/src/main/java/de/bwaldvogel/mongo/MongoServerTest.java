package de.bwaldvogel.mongo;

import static de.bwaldvogel.mongo.backend.TestUtils.toInetSocketAddress;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import de.bwaldvogel.mongo.backend.CollectionUtils;

public abstract class MongoServerTest {

    private static final int TEST_TIMEOUT_SECONDS = 10;

    private MongoServer server;

    protected abstract MongoBackend createBackend() throws Exception;

    @BeforeEach
    void setUp() throws Exception {
        server = new MongoServer(createBackend());
    }

    @AfterEach
    void tearDown() {
        server.shutdown();
    }

    @Test
    void testToString() throws Exception {
        assertThat(server).hasToString("MongoServer()");
        InetSocketAddress inetSocketAddress = server.bind();
        assertThat(server).hasToString("MongoServer(port: " + inetSocketAddress.getPort() + ", ssl: false)");
    }

    @Test
    void testBindAndConnect() throws Exception {
        InetSocketAddress inetSocketAddress = server.bind();
        try (MongoClient mongoClient = MongoClients.create("mongodb://" + inetSocketAddress.getHostString() + ":" + inetSocketAddress.getPort())) {
            mongoClient.getDatabase("abc").createCollection("def");
            assertThat(mongoClient.listDatabaseNames())
                .containsExactly("abc");
        }
    }

    @Test
    void testBindAndGetConnectionStringThenConnect() throws Exception {
        String connectionString = server.bindAndGetConnectionString();
        try (MongoClient mongoClient = MongoClients.create(connectionString)) {
            mongoClient.getDatabase("abc").createCollection("def");
            assertThat(mongoClient.listDatabaseNames())
                .containsExactly("abc");
        }
    }

    @Test
    @Timeout(TEST_TIMEOUT_SECONDS)
    void testStopListening() throws Exception {
        String connectionString = server.bindAndGetConnectionString();

        try (MongoClient client = MongoClients.create(connectionString)) {
            // request something
            pingServer(client);

            server.stopListening();

            // existing clients must still work
            pingServer(client);

            // new clients must fail
            assertThatExceptionOfType(IOException.class)
                .isThrownBy(() -> {
                    try (Socket socket = new Socket()) {
                        String host = CollectionUtils.getSingleElement(new ConnectionString(connectionString).getHosts());
                        socket.connect(new InetSocketAddress(host, 1234));
                    }
                });
        }
    }

    @Test
    @Timeout(TEST_TIMEOUT_SECONDS)
    void testShutdownNow() throws Exception {
        String connectionString = server.bindAndGetConnectionString();
        MongoClient client = MongoClients.create(connectionString);

        // request something to open a connection
        pingServer(client);

        server.shutdownNow();
    }

    @Test
    @Timeout(TEST_TIMEOUT_SECONDS)
    void testGetLocalAddress() throws Exception {
        assertThat(server.getLocalAddress()).isNull();
        InetSocketAddress serverAddress = server.bind();
        InetSocketAddress localAddress = server.getLocalAddress();
        assertThat(localAddress).isEqualTo(serverAddress);
        server.shutdownNow();
        assertThat(server.getLocalAddress()).isNull();
    }

    @Test
    @Timeout(TEST_TIMEOUT_SECONDS)
    void testShutdownAndRestart() throws Exception {
        String connectionString = server.bindAndGetConnectionString();
        InetSocketAddress serverAddress = server.getLocalAddress();

        try (MongoClient client = MongoClients.create(connectionString)) {
            // request something to open a connection
            pingServer(client);

            server.shutdownNow();

            assertThatExceptionOfType(MongoException.class)
                .isThrownBy(() -> pingServer(client));

            // restart
            server.bind(serverAddress);

            pingServer(client);
        }
    }

    @Test
    @Timeout(TEST_TIMEOUT_SECONDS)
    void testSsl() throws Exception {
        server.enableSsl(getPrivateKey(), null, getCertificate());
        String connectionString = server.bindAndGetConnectionString();
        assertThat(connectionString).endsWith("?tls=true");
        InetSocketAddress inetSocketAddress = toInetSocketAddress(connectionString);
        int port = inetSocketAddress.getPort();
        assertThat(server).hasToString("MongoServer(port: " + port + ", ssl: true)");

        SSLContext sslContext = createSslContext(loadTestKeyStore());

        MongoClientSettings clientSettings = MongoClientSettings.builder()
            .applyConnectionString(new ConnectionString(connectionString))
            .applyToSslSettings(builder -> {
                builder.enabled(true);
                builder.context(sslContext);
            })
            .build();

        try (MongoClient client = MongoClients.create(clientSettings)) {
            pingServer(client);
        }
    }

    @Test
    void testEnableSslAfterAlreadyStarted() throws Exception {
        server.bindAndGetConnectionString();

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> server.enableSsl(getPrivateKey(), null, getCertificate()))
            .withMessage("Server already started");
    }

    @Test
    void testEnableSslWithEmptyKeyCertChain() throws Exception {
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> server.enableSsl(null, null))
            .withMessage("Param 'keyCertChain' must not be empty");
    }

    @Test
    void testEnableSslWithMissingPrivateKey() throws Exception {
        X509Certificate certificate = getCertificate();

        assertThatExceptionOfType(NullPointerException.class)
            .isThrownBy(() -> server.enableSsl(null, null, certificate))
            .withMessage("key required for servers");
    }

    private PrivateKey getPrivateKey() throws Exception {
        return (PrivateKey) loadTestKeyStore().getKey("localhost", new char[0]);
    }

    private X509Certificate getCertificate() throws Exception {
        return (X509Certificate) loadTestKeyStore().getCertificate("localhost");
    }

    private KeyStore loadTestKeyStore() throws Exception {
        try (InputStream keyStoreStream = getClass().getResourceAsStream("/test-keystore.jks")) {
            KeyStore keyStore = KeyStore.getInstance("JKS");
            keyStore.load(keyStoreStream, new char[0]);
            return keyStore;
        }
    }

    private SSLContext createSslContext(KeyStore keyStore) throws Exception {
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(keyStore);

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, trustManagerFactory.getTrustManagers(), null);
        return sslContext;
    }

    private void pingServer(MongoClient client) {
        client.getDatabase("admin").runCommand(new Document("ping", 1));
    }

}
