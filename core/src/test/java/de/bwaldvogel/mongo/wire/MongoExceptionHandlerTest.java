package de.bwaldvogel.mongo.wire;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;

class MongoExceptionHandlerTest {

    @Test
    void exceptionCaught() {
        ChannelHandlerContext mockChannelHandlerContext = mock(ChannelHandlerContext.class);
        Channel mockChannel = mock(Channel.class);
        ChannelFuture mockChannelFuture = mock(ChannelFuture.class);

        when(mockChannel.id()).thenReturn(new MockChannelId("channel1"));
        when(mockChannel.close()).thenReturn(mockChannelFuture);
        when(mockChannelHandlerContext.channel()).thenReturn(mockChannel);

        MongoExceptionHandler mongoExceptionHandler = new MongoExceptionHandler();
        mongoExceptionHandler.exceptionCaught(mockChannelHandlerContext, new RuntimeException("testing"));
        verify(mockChannel, times(1)).id();
        verify(mockChannel, times(1)).close();
    }

    static class MockChannelId implements ChannelId {

        private static final long serialVersionUID = 1L;

        private final String id;

        public MockChannelId(String id) {
            this.id = id;
        }

        @Override
        public String asShortText() {
            return id;
        }

        @Override
        public String asLongText() {
            return id;
        }

        @Override
        public int compareTo(ChannelId o) {
            return 0;
        }
    }
}
