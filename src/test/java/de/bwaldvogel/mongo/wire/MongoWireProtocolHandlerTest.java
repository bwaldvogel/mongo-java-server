package de.bwaldvogel.mongo.wire;

import static org.fest.assertions.Assertions.assertThat;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.junit.Before;
import org.junit.Test;

public class MongoWireProtocolHandlerTest {

    private MongoWireProtocolHandler mongoWireProtocolHandler;

    @Before
    public void setUp() {
        mongoWireProtocolHandler = new MongoWireProtocolHandler();
    }

    @Test
    public void testReadStringUnicode() throws Exception {
        String string = "\u0442\u0435\u0441\u0442";
        byte[] bytes = string.getBytes("UTF-8");
        ChannelBuffer buffer = ChannelBuffers.directBuffer(bytes.length + 1);
        buffer.writeBytes(bytes);
        buffer.writeByte(0);
        assertThat(mongoWireProtocolHandler.readCString(buffer)).isEqualTo(string);
    }
}
