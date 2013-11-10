package de.bwaldvogel.mongo.wire;

import static org.fest.assertions.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.bson.BSONObject;
import org.bson.BasicBSONEncoder;
import org.bson.BasicBSONObject;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.junit.Test;

public class BsonDecoderTest {

    @Test
    public void testDecodeStringUnicode() throws Exception {
        String string = "\u0442\u0435\u0441\u0442";
        byte[] bytes = string.getBytes("UTF-8");
        ChannelBuffer buffer = ChannelBuffers.directBuffer(bytes.length + 1);
        buffer.writeBytes(bytes);
        buffer.writeByte(0);
        assertThat(new BsonDecoder().decodeCString(buffer)).isEqualTo(string);
    }

    @Test
    public void testDecodeObjects() throws Exception {
        List<BSONObject> objects = new ArrayList<BSONObject>();
        objects.add(new BasicBSONObject("key", new MaxKey()).append("foo", "bar"));
        objects.add(new BasicBSONObject("key", new MinKey()).append("test", new MaxKey()));

        for (BSONObject object : objects) {
            byte[] encodedData = new BasicBSONEncoder().encode(object);
            ChannelBuffer buf = ChannelBuffers.wrappedBuffer(ChannelBuffers.LITTLE_ENDIAN, encodedData);
            BSONObject decodedObject = new BsonDecoder().decodeBson(buf);
            assertThat(decodedObject).isEqualTo(object);
        }
    }
}
