package de.bwaldvogel.mongo.wire;

import static org.fest.assertions.Assertions.assertThat;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import org.bson.BSONObject;
import org.bson.Document;
import org.bson.BasicBSONEncoder;
import org.bson.BasicBSONObject;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class BsonDecoderTest {

    @Test
    public void testDecodeStringUnicode() throws Exception {
        String string = "\u0442\u0435\u0441\u0442";
        byte[] bytes = string.getBytes("UTF-8");
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeBytes(bytes);
        buffer.writeByte(0);
        assertThat(new BsonDecoder().decodeCString(buffer)).isEqualTo(string);
    }

    @Test
    public void testDecodeObjects() throws Exception {
        List<Document> objects = new ArrayList<Document>();
        objects.add(new Document("key", new MaxKey()).append("foo", "bar"));
        objects.add(new Document("key", new MinKey()).append("test", new MaxKey()));

        for (Document object : objects) {
            // TODO: make more efficient
            BSONObject bsonObject = new BasicBSONObject();
            bsonObject.putAll(object);
            byte[] encodedData = new BasicBSONEncoder().encode(bsonObject);
            ByteBuf buf = Unpooled.wrappedBuffer(encodedData).order(ByteOrder.LITTLE_ENDIAN);
            Document decodedObject = new BsonDecoder().decodeBson(buf);
            assertThat(decodedObject).isEqualTo(object);
        }
    }
}
