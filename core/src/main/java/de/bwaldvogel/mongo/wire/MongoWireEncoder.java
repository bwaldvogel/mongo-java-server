package de.bwaldvogel.mongo.wire;

import java.nio.ByteOrder;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.wire.message.MongoReply;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class MongoWireEncoder extends MessageToByteEncoder<MongoReply> {

    private static final Logger log = LoggerFactory.getLogger(MongoWireEncoder.class);

    private final BsonEncoder bsonEncoder = new BsonEncoder();

    @Override
    protected void encode(ChannelHandlerContext ctx, MongoReply reply, ByteBuf buf) throws Exception {

        ByteBuf out = buf.order(ByteOrder.LITTLE_ENDIAN);

        out.writeInt(0); // write length later

        out.writeInt(reply.getHeader().getRequestID());
        out.writeInt(reply.getHeader().getResponseTo());
        out.writeInt(OpCode.OP_REPLY.getId());

        out.writeInt(reply.getFlags());
        out.writeLong(reply.getCursorId());
        out.writeInt(reply.getStartingFrom());
        final List<Document> documents = reply.getDocuments();
        out.writeInt(documents.size());

        for (Document document : documents) {
            bsonEncoder.encodeDocument(document, out);
        }

        log.debug("wrote reply: {}", reply);

        // now set the length
        final int writerIndex = out.writerIndex();
        out.setInt(0, writerIndex);
    }
}
