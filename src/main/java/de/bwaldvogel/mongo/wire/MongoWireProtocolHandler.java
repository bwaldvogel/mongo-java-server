package de.bwaldvogel.mongo.wire;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.BSONTimestamp;
import org.bson.types.ObjectId;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;

import de.bwaldvogel.mongo.backend.Utils;
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

    public static final int MAX_BSON_OBJECT_SIZE = 16 * 1024 * 1024;
    public static final int MAX_MESSAGE_SIZE_BYTES = 48 * 1000 * 1000;

    private static final Logger log = Logger.getLogger(MongoWireProtocolHandler.class);

    private static final int maxFrameLength = Integer.MAX_VALUE;
    private static final int lengthFieldOffset = 0;
    private static final int lengthFieldLength = 4;
    private static final int lengthAdjustment = -lengthFieldLength;
    private static final int initialBytesToStrip = 0;

    public MongoWireProtocolHandler() {
        super(maxFrameLength, lengthFieldOffset, lengthFieldLength, lengthAdjustment, initialBytesToStrip);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        log.error("exception for client " + e.getChannel().getId(), e.getCause());
        e.getChannel().close();
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {

        if (buffer.readableBytes() < 4) {
            return null;
        }

        buffer.markReaderIndex();
        int totalLength = buffer.readInt();

        if (totalLength > MAX_MESSAGE_SIZE_BYTES) {
            throw new IOException("message too large: " + totalLength + " bytes");
        }

        if (buffer.readableBytes() < totalLength - lengthFieldLength) {
            buffer.resetReaderIndex();
            return null; // retry
        }
        buffer = buffer.readSlice(totalLength - lengthFieldLength);
        int readable = buffer.readableBytes();
        if (readable != totalLength - lengthFieldLength) {
            throw new IllegalStateException();
        }

        final int requestID = buffer.readInt();
        final int responseTo = buffer.readInt();
        final MessageHeader header = new MessageHeader(requestID, responseTo);

        int opCodeId = buffer.readInt();
        final OpCode opCode = OpCode.getById(opCodeId);
        if (opCode == null) {
            throw new IOException("opCode " + opCodeId + " not supported");
        }

        Object ret;

        switch (opCode) {
        case OP_QUERY:
            ret = handleQuery(channel, header, buffer);
            break;
        case OP_INSERT:
            ret = handleInsert(channel, header, buffer);
            break;
        case OP_DELETE:
            ret = handleDelete(channel, header, buffer);
            break;
        case OP_UPDATE:
            ret = handleUpdate(channel, header, buffer);
            break;
        default:
            throw new UnsupportedOperationException("unsupported opcode: " + opCode);
        }

        if (buffer.readable()) {
            throw new IOException();
        }

        return ret;
    }

    private Object handleDelete(Channel channel, MessageHeader header, ChannelBuffer buffer) throws IOException {

        buffer.skipBytes(4); // reserved

        final String fullCollectionName = readCString(buffer);

        final int flags = buffer.readInt();
        boolean singleRemove = false;
        if (flags == 0) {
            // ignore
        } else if (flags == 1) {
            singleRemove = true;
        } else {
            throw new UnsupportedOperationException("flags=" + flags + " not yet supported");
        }

        BSONObject selector = readBSON(buffer);
        log.debug("delete " + selector + " from " + fullCollectionName);
        return new MongoDelete(channel, header, fullCollectionName, selector, singleRemove);
    }

    private Object handleUpdate(Channel channel, MessageHeader header, ChannelBuffer buffer) throws IOException {

        buffer.skipBytes(4); // reserved

        final String fullCollectionName = readCString(buffer);

        final int flags = buffer.readInt();
        boolean upsert = UpdateFlag.UPSERT.isSet(flags);
        boolean multi = UpdateFlag.MULTI_UPDATE.isSet(flags);

        BSONObject selector = readBSON(buffer);
        BSONObject update = readBSON(buffer);
        log.debug("update " + selector + " in " + fullCollectionName);
        return new MongoUpdate(channel, header, fullCollectionName, selector, update, upsert, multi);
    }

    private Object handleInsert(Channel channel, MessageHeader header, ChannelBuffer buffer) throws IOException {

        final int flags = buffer.readInt();
        if (flags != 0)
            throw new UnsupportedOperationException("flags=" + flags + " not yet supported");

        final String fullCollectionName = readCString(buffer);

        List<BSONObject> documents = new ArrayList<BSONObject>();
        while (buffer.readable()) {
            BSONObject document = readBSON(buffer);
            if (document == null) {
                return null;
            }
            documents.add(document);
        }
        log.debug("insert " + documents + " in " + fullCollectionName);
        return new MongoInsert(channel, header, fullCollectionName, documents);
    }

    private Object handleQuery(Channel channel, MessageHeader header, ChannelBuffer buffer) throws IOException {

        int flags = buffer.readInt();

        final String fullCollectionName = readCString(buffer);
        final int numberToSkip = buffer.readInt();
        final int numberToReturn = buffer.readInt();

        BSONObject query = readBSON(buffer);
        BSONObject returnFieldSelector = null;
        if (buffer.readable()) {
            returnFieldSelector = readBSON(buffer);
        }

        MongoQuery mongoQuery = new MongoQuery(channel, header, fullCollectionName, numberToSkip, numberToReturn,
                query, returnFieldSelector);

        if (QueryFlag.SLAVE_OK.isSet(flags)) {
            flags = QueryFlag.SLAVE_OK.removeFrom(flags);
        }

        if (flags != 0)
            throw new UnsupportedOperationException("flags=" + flags + " not yet supported");

        log.debug("query " + query + " from " + fullCollectionName);

        return mongoQuery;
    }

    private BSONObject readBSON(ChannelBuffer buffer) throws IOException {
        // TODO read BSON using Netty
        final int length = buffer.readInt() - 4;
        if (buffer.readableBytes() < length) {
            throw new IOException();
        }
        if (length > MAX_BSON_OBJECT_SIZE) {
            throw new IOException("BSON object too large: " + length + " bytes");
        }

        BSONObject object = new BasicBSONObject();
        int start = buffer.readerIndex();
        while (buffer.readerIndex() - start < length) {
            byte type = buffer.readByte();
            if (type == 0x00) {
                return object;
            }
            String name = readCString(buffer);
            Object value;
            switch (type) {
            case 0x01: // double
                value = Double.valueOf(buffer.readDouble());
                break;
            case 0x02: // utf-8 string
                value = readString(buffer);
                break;
            case 0x03: // embedded document
                value = readBSON(buffer);
                break;
            case 0x04: // array
                value = readArray(buffer);
                break;
            case 0x05: // data
                value = readBinary(buffer);
                break;
            case 0x06: // undefined (deprecated)
                value = null;
                break;
            case 0x07: // object id
                value = readObjectId(buffer);
                break;
            case 0x08: // boolean
                switch (buffer.readByte()) {
                case 0x00:
                    value = Boolean.FALSE;
                    break;
                case 0x01:
                    value = Boolean.TRUE;
                    break;
                default:
                    throw new IOException("illegal boolean value");
                }
                break;
            case 0x09: // UTC datetime
                value = new Date(buffer.readLong());
                break;
            case 0x0A: // null
                value = null;
                break;
            case 0x0B: // regex
                value = readPattern(buffer);
                break;
            case 0x10: // int32
                value = Integer.valueOf(buffer.readInt());
                break;
            case 0x11: // Timestamp
                value = new BSONTimestamp(buffer.readInt(), buffer.readInt());
                break;
            case 0x12: // int64
                value = Long.valueOf(buffer.readLong());
                break;
            default:
                throw new IOException("unknown type: " + type);
            }
            object.put(name, value);
        }
        throw new IOException("illegal BSON object");
    }

    private Pattern readPattern(ChannelBuffer buffer) throws IOException {
        String regex = readCString(buffer);
        String options = readCString(buffer);
        return Utils.createPattern(regex, options);
    }

    private List<Object> readArray(ChannelBuffer buffer) throws IOException {
        List<Object> array = new ArrayList<Object>();
        BSONObject arrayObject = readBSON(buffer);
        for (String key : arrayObject.keySet()) {
            array.add(arrayObject.get(key));
        }
        return array;
    }

    private ObjectId readObjectId(ChannelBuffer buffer) {
        byte[] b = new byte[12];
        buffer.readBytes(b);
        return new ObjectId(b);
    }

    private String readString(ChannelBuffer buffer) throws IOException {
        int length = buffer.readInt();
        byte[] data = new byte[length - 1];
        buffer.readBytes(data);
        String s = new String(data, "UTF-8");
        byte trail = buffer.readByte();
        if (trail != 0x00) {
            throw new IOException();
        }
        return s;
    }

    // default visibility for unit test
    String readCString(ChannelBuffer buffer) throws IOException {
        int length = buffer.bytesBefore((byte) 0);
        if (length < 0)
            throw new IOException("string termination not found");

        String result = buffer.toString(buffer.readerIndex(), length, Charset.forName("UTF-8"));
        buffer.skipBytes(length + 1);
        return result;
    }

    private Object readBinary(ChannelBuffer buffer) throws IOException {
        int length = buffer.readInt();
        int subtype = buffer.readByte();
        switch (subtype) {
        case 0x00:
        case 0x80: {
            byte[] data = new byte[length];
            buffer.readBytes(data);
            return data;
        }
        case 0x03:
        case 0x04:
            if (length != 128 / 8) {
                throw new IOException();
            }
            return new UUID(buffer.readLong(), buffer.readLong());
        default:
            throw new IOException();
        }
    }
}
