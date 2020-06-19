package de.bwaldvogel.mongo.wire;

import java.util.List;

import org.junit.jupiter.api.Test;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.wire.message.MessageHeader;
import de.bwaldvogel.mongo.wire.message.MongoGetMore;
import de.bwaldvogel.mongo.wire.message.MongoMessage;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import de.bwaldvogel.mongo.wire.message.MongoReply;
import io.netty.channel.Channel;

import static de.bwaldvogel.mongo.TestUtils.json;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MongoDatabaseHandlerTest {

    @Test
    void testWrappedCommand() throws Exception {
        final MongoBackend backend = mock(MongoBackend.class);
        final Channel channel = mock(Channel.class);

        final Document queryDoc = json("'$query': { 'count': 'collectionName' }, '$readPreference': { 'mode': 'secondaryPreferred' }");
        final Document subQueryDoc = json("'count': 'collectionName'");
        final MongoQuery query = new MongoQuery(channel, null, "dbName.$cmd", 0, 0, queryDoc, null);

        final MongoDatabaseHandler handler = new MongoDatabaseHandler(backend, null);

        handler.handleCommandAsync(query);

        verify(backend).handleCommandAsync(channel, "dbName", "count", subQueryDoc);
    }

    @Test
    void testNonWrappedCommand() throws Exception {
        final MongoBackend backend = mock(MongoBackend.class);
        final Channel channel = mock(Channel.class);

        final Document queryDoc = json("'count': 'collectionName'");
        final MongoQuery query = new MongoQuery(channel, null, "dbName.$cmd", 0, 0, queryDoc, null);

        final MongoDatabaseHandler handler = new MongoDatabaseHandler(backend, null);

        handler.handleCommandAsync(query);

        verify(backend).handleCommandAsync(channel, "dbName", "count", queryDoc);
    }

    @Test
    void testHandleQueryUnknownCollection() throws Exception {
        final MongoBackend backend = mock(MongoBackend.class);
        when(backend.handleQueryAsync(any())).thenCallRealMethod();

        final Channel channel = mock(Channel.class);

        final MessageHeader header = new MessageHeader(0, 0);
        final MongoQuery query = new MongoQuery(channel, header, "dbName.$cmd.unknown", 0, 0, null, null);

        final MongoDatabaseHandler handler = new MongoDatabaseHandler(backend, null);

        MongoReply responseMongoReply = handler.handleQueryAsync(query).toCompletableFuture().get();

        assertThat(responseMongoReply).isNotNull();
        List<Document> documents = responseMongoReply.getDocuments();
        assertThat(documents).isNotEmpty();
        Document doc = documents.get(0);
        assertThat(doc).isNotNull();
        assertThat(doc.get("$err")).isEqualTo("unknown collection: $cmd.unknown");
        assertThat(doc.get("errmsg")).isEqualTo("unknown collection: $cmd.unknown");
        assertThat(doc.get("ok")).isEqualTo(0);
    }

    @Test
    void testHandleQueryCommandUnknownError() throws Exception {
        final MongoBackend backend = mock(MongoBackend.class);
        when(backend.handleCommandAsync(any(), any(), any(), any())).thenCallRealMethod();
        when(backend.handleCommand(any(), any(), any(), any())).thenThrow(new RuntimeException("unexpected"));

        final Channel channel = mock(Channel.class);

        final MessageHeader header = new MessageHeader(0, 0);
        final Document queryDoc = json("'$query': { 'count': 'collectionName' }, '$readPreference': { 'mode': 'secondaryPreferred' }");
        final MongoQuery query = new MongoQuery(channel, header, "dbName.$cmd", 0, 0, queryDoc, null);

        final MongoDatabaseHandler handler = new MongoDatabaseHandler(backend, null);

        MongoReply responseMongoReply = handler.handleQueryAsync(query).toCompletableFuture().get();

        assertThat(responseMongoReply).isNotNull();
        List<Document> documents = responseMongoReply.getDocuments();
        assertThat(documents).isNotEmpty();
        Document doc = documents.get(0);
        assertThat(doc).isNotNull();
        assertThat(doc.get("$err")).isEqualTo("Unknown error: unexpected");
        assertThat(doc.get("errmsg")).isEqualTo("Unknown error: unexpected");
        assertThat(doc.get("ok")).isEqualTo(0);
    }

    @Test
    void testHandleQueryUnknownError() throws Exception {
        final MongoBackend backend = mock(MongoBackend.class);
        when(backend.handleQueryAsync(any())).thenCallRealMethod();
        when(backend.handleQuery(any())).thenThrow(new RuntimeException("unexpected"));

        final Channel channel = mock(Channel.class);

        final MessageHeader header = new MessageHeader(0, 0);
        final MongoQuery query = new MongoQuery(channel, header, "dbName.find", 0, 0, null, null);

        final MongoDatabaseHandler handler = new MongoDatabaseHandler(backend, null);

        MongoReply responseMongoReply = handler.handleQueryAsync(query).toCompletableFuture().get();

        assertThat(responseMongoReply).isNotNull();
        List<Document> documents = responseMongoReply.getDocuments();
        assertThat(documents).isNotEmpty();
        Document doc = documents.get(0);
        assertThat(doc).isNotNull();
        assertThat(doc.get("$err")).isEqualTo("Unknown error: unexpected");
        assertThat(doc.get("errmsg")).isEqualTo("Unknown error: unexpected");
        assertThat(doc.get("ok")).isEqualTo(0);
    }

    @Test
    void testHandleMessageUnknownError() throws Exception {
        final MongoBackend backend = mock(MongoBackend.class);
        when(backend.handleMessageAsync(any())).thenCallRealMethod();
        when(backend.handleMessage(any())).thenThrow(new RuntimeException("unexpected"));

        final Channel channel = mock(Channel.class);

        final MessageHeader header = new MessageHeader(0, 0);

        final MongoDatabaseHandler handler = new MongoDatabaseHandler(backend, null);

        MongoMessage requestMessage = new MongoMessage(channel, header, new Document("key", "1"));

        MongoMessage responseMessage = handler.handleMessageAsync(requestMessage).toCompletableFuture().get();

        assertThat(responseMessage).isNotNull();
        Document responseMessageDoc = responseMessage.getDocument();
        assertThat(responseMessageDoc).isNotNull();
        assertThat(responseMessageDoc.get("$err")).isEqualTo("Unknown error: unexpected");
        assertThat(responseMessageDoc.get("errmsg")).isEqualTo("Unknown error: unexpected");
        assertThat(responseMessageDoc.get("ok")).isEqualTo(0);
    }

    @Test
    void testHandleGetMoreUnknownError() throws Exception {
        final MongoBackend backend = mock(MongoBackend.class);
        when(backend.handleGetMoreAsync(any())).thenCallRealMethod();
        when(backend.handleGetMore(any())).thenThrow(new RuntimeException("unexpected"));

        final Channel channel = mock(Channel.class);

        final MessageHeader header = new MessageHeader(0, 0);

        final MongoDatabaseHandler handler = new MongoDatabaseHandler(backend, null);

        MongoGetMore requestGetMore = new MongoGetMore(channel, header, "collectionName", 5, 0);

        MongoReply responseMongoReply = handler.handleGetMoreAsync(requestGetMore).toCompletableFuture().get();

        assertThat(responseMongoReply).isNotNull();
        List<Document> documents = responseMongoReply.getDocuments();
        assertThat(documents).isNotEmpty();
        Document doc = documents.get(0);
        assertThat(doc).isNotNull();
        assertThat(doc.get("$err")).isEqualTo("Unknown error: unexpected");
        assertThat(doc.get("errmsg")).isEqualTo("Unknown error: unexpected");
        assertThat(doc.get("ok")).isEqualTo(0);
    }
}
