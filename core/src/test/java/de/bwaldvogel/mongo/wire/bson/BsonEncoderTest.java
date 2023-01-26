package de.bwaldvogel.mongo.wire.bson;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.ObjectId;
import de.bwaldvogel.mongo.wire.BsonConstants;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class BsonEncoderTest {

    @Test
    void testEncodeEmptyDocument() throws Exception {
        Document document = new Document();

        ByteBuf buffer = Unpooled.buffer();
        try {
            BsonEncoder.encodeDocument(document, buffer);
            assertThat(buffer.writerIndex()).isEqualTo(5);
            assertThat(buffer.readerIndex()).isEqualTo(0);
            assertThat(buffer.readIntLE()).isEqualTo(5);
            assertThat(buffer.readByte()).isEqualTo(BsonConstants.TERMINATING_BYTE);
        } finally {
            buffer.release();
        }
    }

    @Test
    void testEncodeDecodeRoundtrip() throws Exception {
        Document document = new Document();
        document.put("key1", "value");
        document.put("key2", 123.0);
        document.put("key3", List.of(1L, 2L));
        document.put("key4", true);
        document.put("key5", UUID.randomUUID());
        document.put("key6", new ObjectId());

        ByteBuf buffer = Unpooled.buffer();
        try {
            BsonEncoder.encodeDocument(document, buffer);
            Document decodedDocument = BsonDecoder.decodeBson(buffer);
            assertThat(decodedDocument).isEqualTo(document);
        } finally {
            buffer.release();
        }
    }

}
