package de.bwaldvogel.mongo.backend;

import static de.bwaldvogel.mongo.backend.AbstractMongoBackend.ADMIN_DB_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.bson.Document;
import io.netty.channel.Channel;

class AbstractMongoBackendTest {

    private MongoBackend backend;
    private MongoBackend backendWithError;

    @BeforeEach
    public void setup() {
        backend = new AbstractMongoBackend() {

            @Override
            protected MongoDatabase openOrCreateDatabase(String databaseName) {
                MongoDatabase mockDatabase = Mockito.mock(AbstractMongoDatabase.class);

                Document fakeResponse = new Document();
                Utils.markOkay(fakeResponse);
                fakeResponse.put("message", "fakeResponse");

                when(mockDatabase.handleCommand(any(), any(), any(), any())).thenReturn(fakeResponse);

                return mockDatabase;
            }
        };

        backendWithError = new AbstractMongoBackend() {

            @Override
            protected MongoDatabase openOrCreateDatabase(String databaseName) {
                return null;
            }

            @Override
            public void dropDatabase(String database) {
                throw new RuntimeException("unexpected");
            }
        };
    }

    @Test
    void testHandleCommand() {
        Channel channel = Mockito.mock(Channel.class);
        when(channel.remoteAddress()).thenReturn(new InetSocketAddress("127.0.1.254", 27017));

        Document response = backend.handleCommand(channel, null, "whatsmyuri", null);
        assertThat(response).isNotNull();
        assertThat(response.get("ok")).isEqualTo(1.0);
        assertThat(response.get("you")).isEqualTo("127.0.1.254:27017");
    }

    @Test
    void testHandleAdminCommand() {
        Channel channel = Mockito.mock(Channel.class);

        Document response = backend.handleCommand(channel, ADMIN_DB_NAME, "ping", null);
        assertThat(response).isNotNull();
        assertThat(response.get("ok")).isEqualTo(1.0);
    }

    @Test
    void testMongoDatabaseHandleCommand() {
        Channel channel = Mockito.mock(Channel.class);

        Document response = backend.handleCommand(channel, "mockDatabase", "find", null);
        assertThat(response).isNotNull();
        assertThat(response.get("ok")).isEqualTo(1.0);
        assertThat(response.get("message")).isEqualTo("fakeResponse");
    }

}
