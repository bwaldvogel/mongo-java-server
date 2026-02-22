package de.bwaldvogel.mongo.wire;

import static de.bwaldvogel.mongo.TestUtils.json;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import de.bwaldvogel.mongo.backend.Command;

import de.bwaldvogel.mongo.backend.DatabaseCommand;

import org.junit.jupiter.api.Test;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.wire.message.MessageHeader;
import de.bwaldvogel.mongo.wire.message.MongoMessage;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import io.netty.channel.Channel;

class MongoDatabaseHandlerTest {

    @Test
    void testWrappedCommand() throws Exception {
        MongoBackend backend = mock(MongoBackend.class);
        Channel channel = mock(Channel.class);

        Document queryDoc = json("'$query': { 'count': 'collectionName' }, '$readPreference': { 'mode': 'secondaryPreferred' }");
        Document subQueryDoc = json("'count': 'collectionName'");
        MongoQuery query = new MongoQuery(channel, null, "dbName.$cmd", 0, 0, queryDoc, null);

        MongoDatabaseHandler handler = new MongoDatabaseHandler(backend, null);

        handler.handleCommand(query);

        verify(backend).handleCommand(channel, "dbName", DatabaseCommand.of(Command.COUNT), subQueryDoc);
    }

    @Test
    void testNonWrappedCommand() throws Exception {
        MongoBackend backend = mock(MongoBackend.class);
        Channel channel = mock(Channel.class);

        Document queryDoc = json("'count': 'collectionName'");
        MongoQuery query = new MongoQuery(channel, null, "dbName.$cmd", 0, 0, queryDoc, null);

        MongoDatabaseHandler handler = new MongoDatabaseHandler(backend, null);

        handler.handleCommand(query);

        verify(backend).handleCommand(channel, "dbName", DatabaseCommand.of(Command.COUNT), queryDoc);
    }

    @Test
    void testHandleMessageUnknownError() throws Exception {
        MongoBackend backend = mock(MongoBackend.class);
        when(backend.handleMessage(any())).thenThrow(new RuntimeException("unexpected"));

        Channel channel = mock(Channel.class);

        MessageHeader header = new MessageHeader(0, 0);

        MongoDatabaseHandler handler = new MongoDatabaseHandler(backend, null);

        MongoMessage requestMessage = new MongoMessage(channel, header, new Document("key", "1"));

        MongoMessage responseMessage = handler.handleMessage(requestMessage);

        assertThat(responseMessage).isNotNull();
        Document responseMessageDoc = responseMessage.getDocument();
        assertThat(responseMessageDoc).isNotNull();
        assertThat(responseMessageDoc.get("$err")).isEqualTo("Unknown error: unexpected");
        assertThat(responseMessageDoc.get("errmsg")).isEqualTo("Unknown error: unexpected");
        assertThat(responseMessageDoc.get("ok")).isEqualTo(0);
    }
}
