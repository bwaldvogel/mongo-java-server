package de.bwaldvogel.mongo.wire;

import java.nio.ByteOrder;
import java.util.List;

import org.bson.BSON;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import de.bwaldvogel.mongo.wire.message.MongoReply;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class MongoWireEncoder extends MessageToByteEncoder<MongoReply> {

    private static final Logger log = LoggerFactory.getLogger(MongoWireEncoder.class);

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
            // TODO: make more efficient
            DBObject dbObject = new BasicDBObject();
            dbObject.putAll(document);
            out.writeBytes(BSON.encode(dbObject));
        }

        log.debug("wrote reply: {}", reply);

        // now set the length
        final int writerIndex = out.writerIndex();
        out.setInt(0, writerIndex);
    }
}
