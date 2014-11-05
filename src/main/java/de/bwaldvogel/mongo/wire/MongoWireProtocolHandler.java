package de.bwaldvogel.mongo.wire;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import org.bson.BSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.wire.message.ClientRequest;
import de.bwaldvogel.mongo.wire.message.MessageHeader;
import de.bwaldvogel.mongo.wire.message.MongoDelete;
import de.bwaldvogel.mongo.wire.message.MongoInsert;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import de.bwaldvogel.mongo.wire.message.MongoUpdate;

/**
 * Based on information from <a
 * href="http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol"
 * >http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol</a>
 */
public class MongoWireProtocolHandler extends LengthFieldBasedFrameDecoder {

    public static final int MAX_MESSAGE_SIZE_BYTES = 48 * 1000 * 1000;

    private static final Logger log = LoggerFactory.getLogger(MongoWireProtocolHandler.class);

    private static final int maxFrameLength = Integer.MAX_VALUE;
    private static final int lengthFieldOffset = 0;
    private static final int lengthFieldLength = 4;
    private static final int lengthAdjustment = -lengthFieldLength;
    private static final int initialBytesToStrip = 0;

    private final BsonDecoder bsonDecoder;

    public MongoWireProtocolHandler() {
        super(maxFrameLength, lengthFieldOffset, lengthFieldLength, lengthAdjustment, initialBytesToStrip);
        bsonDecoder = new BsonDecoder();
    }

    @Override
    protected ClientRequest decode(ChannelHandlerContext ctx, ByteBuf buf) throws Exception {

        ByteBuf in = buf.order(ByteOrder.LITTLE_ENDIAN);

        if (in.readableBytes() < 4) {
            return null;
        }

        in.markReaderIndex();
        int totalLength = in.readInt();

        if (totalLength > MAX_MESSAGE_SIZE_BYTES) {
            throw new IOException("message too large: " + totalLength + " bytes");
        }

        if (in.readableBytes() < totalLength - lengthFieldLength) {
            in.resetReaderIndex();
            return null; // retry
        }
        in = in.readSlice(totalLength - lengthFieldLength);
        int readable = in.readableBytes();
        if (readable != totalLength - lengthFieldLength) {
            throw new IllegalStateException();
        }

        final int requestID = in.readInt();
        final int responseTo = in.readInt();
        final MessageHeader header = new MessageHeader(requestID, responseTo);

        int opCodeId = in.readInt();
        final OpCode opCode = OpCode.getById(opCodeId);
        if (opCode == null) {
            throw new IOException("opCode " + opCodeId + " not supported");
        }

        final Channel channel = ctx.channel();
        final ClientRequest ret;

        switch (opCode) {
        case OP_QUERY:
            ret = handleQuery(channel, header, in);
            break;
        case OP_INSERT:
            ret = handleInsert(channel, header, in);
            break;
        case OP_DELETE:
            ret = handleDelete(channel, header, in);
            break;
        case OP_UPDATE:
            ret = handleUpdate(channel, header, in);
            break;
        default:
            throw new UnsupportedOperationException("unsupported opcode: " + opCode);
        }

        if (in.isReadable()) {
            throw new IOException();
        }

        return ret;
    }

    private ClientRequest handleDelete(Channel channel, MessageHeader header, ByteBuf buffer) throws IOException {

        buffer.skipBytes(4); // reserved

        final String fullCollectionName = bsonDecoder.decodeCString(buffer);

        final int flags = buffer.readInt();
        boolean singleRemove = false;
        if (flags == 0) {
            // ignore
        } else if (flags == 1) {
            singleRemove = true;
        } else {
            throw new UnsupportedOperationException("flags=" + flags + " not yet supported");
        }

        BSONObject selector = bsonDecoder.decodeBson(buffer);
        log.debug("delete {} from {}", selector, fullCollectionName);
        return new MongoDelete(channel, header, fullCollectionName, selector, singleRemove);
    }

    private ClientRequest handleUpdate(Channel channel, MessageHeader header, ByteBuf buffer) throws IOException {

        buffer.skipBytes(4); // reserved

        final String fullCollectionName = bsonDecoder.decodeCString(buffer);

        final int flags = buffer.readInt();
        boolean upsert = UpdateFlag.UPSERT.isSet(flags);
        boolean multi = UpdateFlag.MULTI_UPDATE.isSet(flags);

        BSONObject selector = bsonDecoder.decodeBson(buffer);
        BSONObject update = bsonDecoder.decodeBson(buffer);
        log.debug("update {} in {}", selector, fullCollectionName);
        return new MongoUpdate(channel, header, fullCollectionName, selector, update, upsert, multi);
    }

    private ClientRequest handleInsert(Channel channel, MessageHeader header, ByteBuf buffer) throws IOException {

        final int flags = buffer.readInt();
        if (flags != 0)
            throw new UnsupportedOperationException("flags=" + flags + " not yet supported");

        final String fullCollectionName = bsonDecoder.decodeCString(buffer);

        List<BSONObject> documents = new ArrayList<BSONObject>();
        while (buffer.isReadable()) {
            BSONObject document = bsonDecoder.decodeBson(buffer);
            if (document == null) {
                return null;
            }
            documents.add(document);
        }
        log.debug("insert {} in {}", documents, fullCollectionName);
        return new MongoInsert(channel, header, fullCollectionName, documents);
    }

    private ClientRequest handleQuery(Channel channel, MessageHeader header, ByteBuf buffer) throws IOException {

        int flags = buffer.readInt();

        final String fullCollectionName = bsonDecoder.decodeCString(buffer);
        final int numberToSkip = buffer.readInt();
        final int numberToReturn = buffer.readInt();

        BSONObject query = bsonDecoder.decodeBson(buffer);
        BSONObject returnFieldSelector = null;
        if (buffer.isReadable()) {
            returnFieldSelector = bsonDecoder.decodeBson(buffer);
        }

        MongoQuery mongoQuery = new MongoQuery(channel, header, fullCollectionName, numberToSkip, numberToReturn,
                query, returnFieldSelector);

        if (QueryFlag.SLAVE_OK.isSet(flags)) {
            flags = QueryFlag.SLAVE_OK.removeFrom(flags);
        }

        if (QueryFlag.NO_CURSOR_TIMEOUT.isSet(flags)) {
            flags = QueryFlag.NO_CURSOR_TIMEOUT.removeFrom(flags);
        }
        
        if (flags != 0)
            throw new UnsupportedOperationException("flags=" + flags + " not yet supported");

        log.debug("query {} from {}", query, fullCollectionName);

        return mongoQuery;
    }

}
