package de.bwaldvogel.mongo.wire;

import static org.fest.assertions.Assertions.assertThat;

import java.util.Arrays;
import java.util.UUID;

import org.junit.Test;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.ObjectId;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class BsonEncoderTest {

    @Test
    public void testEncodeEmptyDocument() throws Exception {
        Document document = new Document();

        ByteBuf buffer = Unpooled.buffer();
        try {
            new BsonEncoder().encodeDocument(document, buffer);
            assertThat(buffer.writerIndex()).isEqualTo(5);
            assertThat(buffer.readerIndex()).isEqualTo(0);
            assertThat(buffer.readInt()).isEqualTo(5);
            assertThat(buffer.readByte()).isEqualTo(BsonConstants.TERMINATING_BYTE);
        } finally {
            buffer.release();
        }
    }

    @Test
    public void testEncodeDecodeRoundtrip() throws Exception {
        Document document = new Document();
        document.put("key1", "value");
        document.put("key2", 123.0);
        document.put("key3", Arrays.asList(1L, 2L));
        document.put("key4", true);
        document.put("key5", UUID.randomUUID());
        document.put("key6", new ObjectId());

        ByteBuf buffer = Unpooled.buffer();
        try {
            new BsonEncoder().encodeDocument(document, buffer);

            Document decodedDocument = new BsonDecoder().decodeBson(buffer);
            assertThat(decodedDocument).isEqualTo(document);
        } finally {
            buffer.release();
        }
    }

}