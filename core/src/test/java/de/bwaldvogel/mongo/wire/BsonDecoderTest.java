package de.bwaldvogel.mongo.wire;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.MaxKey;
import de.bwaldvogel.mongo.bson.MinKey;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class BsonDecoderTest {

    @Test
    public void testDecodeStringUnicode() throws Exception {
        String string = "\u0442\u0435\u0441\u0442";
        byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
        ByteBuf buffer = Unpooled.buffer();
        try {
            buffer.writeBytes(bytes);
            buffer.writeByte(0);
            assertThat(new BsonDecoder().decodeCString(buffer)).isEqualTo(string);
        } finally {
            buffer.release();
        }
    }

    @Test
    public void testEncodeDecodeRoundtrip() throws Exception {
        List<Document> objects = new ArrayList<>();
        objects.add(new Document("key", MaxKey.getInstance()).append("foo", "bar"));
        objects.add(new Document("key", MinKey.getInstance()).append("test", MaxKey.getInstance()));

        for (Document document : objects) {
            ByteBuf buffer = Unpooled.buffer();
            try {
                new BsonEncoder().encodeDocument(document, buffer);
                Document decodedObject = new BsonDecoder().decodeBson(buffer);
                assertThat(decodedObject).isEqualTo(document);
            } finally {
                buffer.release();
            }
        }
    }
}
