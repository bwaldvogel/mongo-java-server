package de.bwaldvogel.mongo.backend;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.CursorNotFoundException;
import de.bwaldvogel.mongo.wire.message.MongoGetMore;
import de.bwaldvogel.mongo.wire.message.MongoKillCursors;
import io.netty.channel.Channel;
import org.mockito.Mockito;

import static de.bwaldvogel.mongo.backend.AbstractMongoBackend.ADMIN_DB_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

class AbstractMongoBackendTest {

    private MongoBackend backend;
    private MongoBackend backendWithError;
    private CursorRegistry cursorRegistry;

    @BeforeEach
    public void setup() {
        backend = new AbstractMongoBackend() {

            {
                AbstractMongoBackendTest.this.cursorRegistry = getCursorRegistry();
            }

            @Override
            protected MongoDatabase openOrCreateDatabase(String databaseName) {
                MongoDatabase mockDatabase = Mockito.mock(AbstractMongoDatabase.class);

                Document fakeResponse = new Document();
                Utils.markOkay(fakeResponse);
                fakeResponse.put("message", "fakeResponse");

                when(mockDatabase.handleCommand(any(), any(), any(), any(), any())).thenReturn(fakeResponse);

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
    void testGetMore_shouldDeleteCursorIfEmpty() {
        List<Document> documents = Arrays.asList(
            new Document("name", "Joe"),
            new Document("name", "Mary"),
            new Document("name", "Steve"));
        Cursor cursor = new InMemoryCursor(cursorRegistry.generateCursorId(), documents);
        cursorRegistry.add(cursor);
        MongoGetMore getMore = new MongoGetMore(null, null, "testcoll", 3,
            cursor.getId());
        backend.handleGetMore(getMore.getCursorId(), getMore.getNumberToReturn());

        assertThatExceptionOfType(CursorNotFoundException.class)
            .isThrownBy(() -> cursorRegistry.getCursor(cursor.getId()))
            .withMessage("[Error 43] Cursor id 1 does not exists");
    }

    @Test
    void testHandleKillCursor() {
        InMemoryCursor cursor1 = new InMemoryCursor(cursorRegistry.generateCursorId(), Collections.singletonList(new Document()));
        InMemoryCursor cursor2 = new InMemoryCursor(cursorRegistry.generateCursorId(), Collections.singletonList(new Document()));
        cursorRegistry.add(cursor1);
        cursorRegistry.add(cursor2);
        assertThat(cursorRegistry.getCursor(cursor1.getId())).isNotNull();
        assertThat(cursorRegistry.getCursor(cursor2.getId())).isNotNull();

        MongoKillCursors killCursors = new MongoKillCursors(null, null, Collections.singletonList(cursor1.getId()));
        backend.handleKillCursors(killCursors);

        assertThatExceptionOfType(CursorNotFoundException.class)
            .isThrownBy(() -> cursorRegistry.getCursor(cursor1.getId()))
            .withMessage("[Error 43] Cursor id 1 does not exists");

        assertThat(cursorRegistry.getCursor(cursor2.getId())).isNotNull();
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

    @Test
    void testHandleCommandAsyncDropDatabase() throws Exception {
        Channel channel = Mockito.mock(Channel.class);

        CompletionStage<Document> responseFuture = backend.handleCommandAsync(channel, "mockDatabase", "dropDatabase", null);
        Document response = responseFuture.toCompletableFuture().get();
        assertThat(response).isNotNull();
        assertThat(response.get("ok")).isEqualTo(1.0);
        assertThat(response.get("dropped")).isEqualTo("mockDatabase");
    }

    @Test
    void testHandleCommandAsyncDropDatabaseError() throws Exception {
        Channel channel = Mockito.mock(Channel.class);

        CompletionStage<Document> responseFuture = backendWithError.handleCommandAsync(channel, "mockDatabase", "dropDatabase", null);
        Document response = responseFuture.toCompletableFuture().get();
        assertThat(response).isNotNull();
        assertThat(response.get("ok")).isEqualTo(0.0);
        assertThat(response.get("dropped")).isEqualTo("mockDatabase");
        assertThat(response.get("errmsg")).isEqualTo("unexpected");
    }
}
