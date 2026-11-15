package de.bwaldvogel.mongo.backend;

import static de.bwaldvogel.mongo.backend.AbstractMongoBackend.ADMIN_DB_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.bson.Document;

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
    void testHandleAdminCommand() {
        Document response = backend.handleCommand(ADMIN_DB_NAME, DatabaseCommand.of(Command.PING), null);
        assertThat(response).isNotNull();
        assertThat(response.get("ok")).isEqualTo(1.0);
    }

    @Test
    void testMongoDatabaseHandleCommand() {
        Document response = backend.handleCommand("mockDatabase", DatabaseCommand.of(Command.FIND), null);
        assertThat(response).isNotNull();
        assertThat(response.get("ok")).isEqualTo(1.0);
        assertThat(response.get("message")).isEqualTo("fakeResponse");
    }

}
