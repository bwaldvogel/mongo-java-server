package de.bwaldvogel.mongo.wire;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.wire.bson.BsonEncoder;
import de.bwaldvogel.mongo.wire.message.MongoMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class MongoWireMessageEncoder extends MessageToByteEncoder<MongoMessage> {

    private static final Logger log = LoggerFactory.getLogger(MongoWireMessageEncoder.class);

    @Override
    protected void encode(ChannelHandlerContext ctx, MongoMessage message, ByteBuf buf) {
        buf.writeIntLE(0); // write length later

        buf.writeIntLE(message.getHeader().getRequestID());
        buf.writeIntLE(message.getHeader().getResponseTo());
        buf.writeIntLE(OpCode.OP_MSG.getId());

        buf.writeIntLE(message.getFlags());
        buf.writeByte(MongoMessage.SECTION_KIND_BODY);

        Document document = message.getDocument();
        try {
            BsonEncoder.encodeDocument(document, buf);
        } catch (RuntimeException e) {
            log.error("Failed to encode {}", document, e);
            ctx.channel().close();
            throw e;
        }

        log.debug("wrote message: {}", message);

        // now set the length
        int writerIndex = buf.writerIndex();
        buf.setIntLE(0, writerIndex);
    }
}
