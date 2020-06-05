package de.bwaldvogel.mongo.wire;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import de.bwaldvogel.mongo.backend.Missing;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.wire.message.MessageHeader;
import de.bwaldvogel.mongo.wire.message.MongoMessage;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

@ExtendWith(MockitoExtension.class)
public class MongoWireMessageEncoderTest {

    @Mock
    private ChannelHandlerContext ctx;

    @Mock
    private Channel channel;

    @BeforeEach
    void setUpContext() throws Exception {
        when(ctx.channel()).thenReturn(channel);
    }

    @Test
    void testExceptionHandling() throws Exception {
        MongoWireMessageEncoder mongoWireEncoder = new MongoWireMessageEncoder();

        MessageHeader header = new MessageHeader(0, 0);
        MongoMessage reply = new MongoMessage(channel, header, new Document("key", Missing.getInstance()));

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> mongoWireEncoder.encode(ctx, reply, Unpooled.buffer()))
            .withMessageContaining("Unknown type: class de.bwaldvogel.mongo.backend.Missing");

        verify(channel).close();
    }

}
