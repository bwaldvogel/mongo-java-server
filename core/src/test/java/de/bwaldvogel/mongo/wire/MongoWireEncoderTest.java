package de.bwaldvogel.mongo.wire;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import de.bwaldvogel.mongo.backend.Missing;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.wire.message.MessageHeader;
import de.bwaldvogel.mongo.wire.message.MongoReply;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

@RunWith(MockitoJUnitRunner.class)
public class MongoWireEncoderTest {

    @Mock
    private ChannelHandlerContext ctx;

    @Mock
    private Channel channel;

    @Before
    public void setUpContext() throws Exception {
        when(ctx.channel()).thenReturn(channel);
    }

    @Test
    public void testExceptionHandling() throws Exception {
        MongoWireEncoder mongoWireEncoder = new MongoWireEncoder();

        MessageHeader header = new MessageHeader(0, 0);
        MongoReply reply = new MongoReply(header, new Document("key", Missing.getInstance()));

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> mongoWireEncoder.encode(ctx, reply, Unpooled.buffer()))
            .withMessageContaining("Unknown type: class de.bwaldvogel.mongo.backend.Missing");

        verify(channel).close();
    }

}
