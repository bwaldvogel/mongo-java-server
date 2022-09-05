package de.bwaldvogel.mongo.wire;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.backend.Assert;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.wire.bson.BsonDecoder;
import de.bwaldvogel.mongo.wire.message.ClientRequest;
import de.bwaldvogel.mongo.wire.message.MessageHeader;
import de.bwaldvogel.mongo.wire.message.MongoMessage;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

/**
 * Based on information from
 * <a href="https://docs.mongodb.org/manual/reference/mongodb-wire-protocol/">https://docs.mongodb.org/manual/reference/mongodb-wire-protocol/</a>
 */
public class MongoWireProtocolHandler extends LengthFieldBasedFrameDecoder {

    private static final Logger log = LoggerFactory.getLogger(MongoWireProtocolHandler.class);

    public static final int MAX_MESSAGE_SIZE_BYTES = 48 * 1000 * 1000;
    public static final int MAX_WRITE_BATCH_SIZE = 1000;

    private static final int MAX_FRAME_LENGTH = Integer.MAX_VALUE;
    private static final int LENGTH_FIELD_OFFSET = 0;
    private static final int LENGTH_FIELD_LENGTH = 4;
    private static final int LENGTH_ADJUSTMENT = -LENGTH_FIELD_LENGTH;
    private static final int INITIAL_BYTES_TO_STRIP = 0;
    private static final int CHECKSUM_LENGTH = 4;

    public MongoWireProtocolHandler() {
        super(MAX_FRAME_LENGTH, LENGTH_FIELD_OFFSET, LENGTH_FIELD_LENGTH, LENGTH_ADJUSTMENT, INITIAL_BYTES_TO_STRIP);
    }

    @Override
    protected ClientRequest decode(ChannelHandlerContext ctx, ByteBuf buf) throws Exception {
        ByteBuf in = buf;

        if (in.readableBytes() < 4) {
            return null;
        }

        in.markReaderIndex();
        int totalLength = in.readIntLE();

        if (totalLength > MAX_MESSAGE_SIZE_BYTES) {
            throw new IOException("message too large: " + totalLength + " bytes");
        }

        if (in.readableBytes() < totalLength - LENGTH_FIELD_LENGTH) {
            in.resetReaderIndex();
            return null; // retry
        }
        in = in.readSlice(totalLength - LENGTH_FIELD_LENGTH);
        long readable = in.readableBytes();
        Assert.equals(readable, totalLength - LENGTH_FIELD_LENGTH);

        final int requestID = in.readIntLE();
        final int responseTo = in.readIntLE();
        final MessageHeader header = new MessageHeader(totalLength, requestID, responseTo);

        int opCodeId = in.readIntLE();
        OpCode opCode = OpCode.getById(opCodeId);
        if (opCode == null) {
            throw new IOException("opCode " + opCodeId + " not supported");
        }

        final Channel channel = ctx.channel();
        final ClientRequest request;

        switch (opCode) {
            case OP_QUERY:
                request = handleQuery(channel, header, in);
                break;
            case OP_MSG:
                request = handleMessage(channel, header, in);
                break;
            default:
                throw new UnsupportedOperationException("unsupported opcode: " + opCode);
        }

        if (in.isReadable()) {
            throw new IOException();
        }

        log.debug("{}", request);

        return request;
    }

    private ClientRequest handleQuery(Channel channel, MessageHeader header, ByteBuf buffer) {
        int flags = buffer.readIntLE();

        final String fullCollectionName = BsonDecoder.decodeCString(buffer);
        final int numberToSkip = buffer.readIntLE();
        final int numberToReturn = buffer.readIntLE();

        Document query = BsonDecoder.decodeBson(buffer);
        Document returnFieldSelector = null;
        if (buffer.isReadable()) {
            returnFieldSelector = BsonDecoder.decodeBson(buffer);
        }

        MongoQuery mongoQuery = new MongoQuery(channel, header, fullCollectionName, numberToSkip, numberToReturn,
            query, returnFieldSelector);

        if (QueryFlag.SLAVE_OK.isSet(flags)) {
            flags = QueryFlag.SLAVE_OK.removeFrom(flags);
        }

        if (QueryFlag.NO_CURSOR_TIMEOUT.isSet(flags)) {
            flags = QueryFlag.NO_CURSOR_TIMEOUT.removeFrom(flags);
        }

        if (flags != 0) {
            throw new UnsupportedOperationException("flags=" + flags + " not yet supported");
        }

        log.debug("query {} from {}", query, fullCollectionName);

        return mongoQuery;
    }

    private ClientRequest handleMessage(Channel channel, MessageHeader header, ByteBuf buffer) {
        int flagBits = buffer.readIntLE();

        Set<MessageFlag> flags = EnumSet.noneOf(MessageFlag.class);
        if (MessageFlag.CHECKSUM_PRESENT.isSet(flagBits)) {
            flagBits = MessageFlag.CHECKSUM_PRESENT.removeFrom(flagBits);
            flags.add(MessageFlag.CHECKSUM_PRESENT);
        }

        if (flagBits != 0) {
            throw new UnsupportedOperationException("flags=" + flagBits + " not yet supported");
        }

        int expectedPayloadSize = header.getTotalLength() - LENGTH_FIELD_LENGTH;
        if (flags.contains(MessageFlag.CHECKSUM_PRESENT)) {
            expectedPayloadSize -= CHECKSUM_LENGTH;
        }

        Document body = null;
        Document documentSequence = new Document();
        while (buffer.readerIndex() < expectedPayloadSize) {
            byte sectionKind = buffer.readByte();
            switch (sectionKind) {
                case MongoMessage.SECTION_KIND_BODY:
                    Assert.isNull(body);
                    body = BsonDecoder.decodeBson(buffer);
                    break;
                case MongoMessage.SECTION_KIND_DOCUMENT_SEQUENCE:
                    decodeKindDocumentSequence(buffer, documentSequence);
                    break;
                default:
                    throw new IllegalArgumentException("Unexpected section kind: " + sectionKind);
            }
        }

        if (flags.contains(MessageFlag.CHECKSUM_PRESENT)) {
            int checksum = buffer.readIntLE();
            log.trace("Ignoring checksum {}", checksum);
        }

        Assert.notNull(body);
        for (Map.Entry<String, Object> entry : documentSequence.entrySet()) {
            Object old = body.put(entry.getKey(), entry.getValue());
            Assert.isNull(old);
        }

        return new MongoMessage(channel, header, body);
    }

    private void decodeKindDocumentSequence(ByteBuf buffer, Document documentSequence) {
        int readerStartOffset = buffer.readerIndex();
        int sectionSize = buffer.readIntLE();
        String documentIdentifier = BsonDecoder.decodeCString(buffer);
        List<Document> documents = new ArrayList<>();
        do {
            Document subDocument = BsonDecoder.decodeBson(buffer);
            documents.add(subDocument);
        } while (buffer.readerIndex() - readerStartOffset < sectionSize);

        Assert.notEmpty(documents);
        Object old = documentSequence.put(documentIdentifier, documents);
        Assert.isNull(old);
    }

}
