package de.bwaldvogel.mongo.wire;

import static de.bwaldvogel.mongo.TestUtils.json;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;

import de.bwaldvogel.mongo.backend.Command;

import de.bwaldvogel.mongo.backend.DatabaseCommand;

import org.junit.jupiter.api.Test;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.wire.message.MessageFlag;
import de.bwaldvogel.mongo.wire.message.MessageHeader;
import de.bwaldvogel.mongo.wire.message.MongoMessage;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

class MongoDatabaseHandlerTest {

    @Test
    void testWrappedCommand() throws Exception {
        MongoBackend backend = mock(MongoBackend.class);

        Document queryDoc = json("'$query': { 'count': 'collectionName' }, '$readPreference': { 'mode': 'secondaryPreferred' }");
        Document subQueryDoc = json("'count': 'collectionName'");
        MongoQuery query = new MongoQuery(null, "dbName.$cmd", 0, 0, queryDoc, null);

        MongoDatabaseHandler handler = new MongoDatabaseHandler(backend, null);

        handler.handleCommand(query, null);

        verify(backend).handleCommand("dbName", DatabaseCommand.of(Command.COUNT), subQueryDoc);
    }

    @Test
    void testNonWrappedCommand() throws Exception {
        MongoBackend backend = mock(MongoBackend.class);

        Document queryDoc = json("'count': 'collectionName'");
        MongoQuery query = new MongoQuery(null, "dbName.$cmd", 0, 0, queryDoc, null);

        MongoDatabaseHandler handler = new MongoDatabaseHandler(backend, null);

        handler.handleCommand(query, null);

        verify(backend).handleCommand("dbName", DatabaseCommand.of(Command.COUNT), queryDoc);
    }

    @Test
    void testHandleMessageUnknownError() throws Exception {
        MongoBackend backend = mock(MongoBackend.class);
        when(backend.handleMessage(any())).thenThrow(new RuntimeException("unexpected"));

        MessageHeader header = new MessageHeader(0, 0);

        MongoDatabaseHandler handler = new MongoDatabaseHandler(backend, null);

        MongoMessage requestMessage = new MongoMessage(header, new Document("key", "1"));

        MongoMessage responseMessage = handler.handleMessage(requestMessage, null);

        assertThat(responseMessage).isNotNull();
        Document responseMessageDoc = responseMessage.getDocument();
        assertThat(responseMessageDoc).isNotNull();
        assertThat(responseMessageDoc.get("$err")).isEqualTo("Unknown error: unexpected");
        assertThat(responseMessageDoc.get("errmsg")).isEqualTo("Unknown error: unexpected");
        assertThat(responseMessageDoc.get("ok")).isEqualTo(0);
    }

    @Test
    void testMessageWithMoreToComeIsHandledWithoutReply() throws Exception {
        MongoBackend backend = mock(MongoBackend.class);
        Channel channel = mock(Channel.class);
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(channel);

        MongoDatabaseHandler handler = new MongoDatabaseHandler(backend, null);
        MongoMessage requestMessage = new MongoMessage(new MessageHeader(0, 0), json("insert: 'coll'"),
            Set.of(MessageFlag.MORE_TO_COME));

        handler.channelRead0(ctx, requestMessage);

        verify(backend).handleMessage(requestMessage);
        verify(channel, never()).writeAndFlush(any());
    }

    @Test
    void testMessageWithoutMoreToComeIsReplied() throws Exception {
        MongoBackend backend = mock(MongoBackend.class);
        Channel channel = mock(Channel.class);
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(channel);

        MongoDatabaseHandler handler = new MongoDatabaseHandler(backend, null);
        MongoMessage requestMessage = new MongoMessage(new MessageHeader(0, 0), json("insert: 'coll'"));

        handler.channelRead0(ctx, requestMessage);

        verify(channel).writeAndFlush(any(MongoMessage.class));
    }

    @Test
    void testWhatsMyUriMessage() {
        MongoBackend backend = mock(MongoBackend.class);
        MongoDatabaseHandler handler = new MongoDatabaseHandler(backend, null);
        MongoMessage request = new MongoMessage(new MessageHeader(1, 0), json("whatsmyuri: 1, $db: 'admin'"));

        MongoMessage response = handler.handleMessage(request, new InetSocketAddress("127.0.1.254", 27017));

        assertThat(response.getDocument()).isEqualTo(json("you: '127.0.1.254:27017', ok: 1.0"));
        verify(backend, never()).handleMessage(any());
    }

    @Test
    void testWhatsMyUriQuery() {
        MongoBackend backend = mock(MongoBackend.class);
        MongoDatabaseHandler handler = new MongoDatabaseHandler(backend, null);
        for (Document document : List.of(json("whatsmyuri: 1"), json("$query: {whatsmyuri: 1}"))) {
            MongoQuery query = new MongoQuery(null, "admin.$cmd", 0, 0, document, null);

            Document response = handler.handleCommand(query, new InetSocketAddress("127.0.1.254", 27017));

            assertThat(response).isEqualTo(json("you: '127.0.1.254:27017', ok: 1.0"));
        }
    }

}
