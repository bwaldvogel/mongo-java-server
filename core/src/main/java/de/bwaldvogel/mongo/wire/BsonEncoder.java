package de.bwaldvogel.mongo.wire;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.bson.BsonRegularExpression;
import org.bson.Document;
import org.bson.types.BSONTimestamp;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;

import io.netty.buffer.ByteBuf;

public class BsonEncoder {

    public void encodeDocument(Document document, ByteBuf out) throws IOException {
        int indexBefore = out.writerIndex();
        out.writeInt(0); // total number of bytes will be written later

        for (String key : document.keySet()) {
            encodeValue(key, document.get(key), out);
        }

        out.writeByte(BsonConstants.TERMINATING_BYTE);
        int indexAfter = out.writerIndex();
        out.writerIndex(indexBefore);
        out.writeInt(indexAfter - indexBefore);
        out.writerIndex(indexAfter);
    }

    private void encodeCString(String data, ByteBuf buffer) throws IOException {
        byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        buffer.writeBytes(bytes);
        buffer.writeByte(BsonConstants.STRING_TERMINATION);
    }

    private void encodeString(String data, ByteBuf buffer) throws IOException {
        byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        buffer.writeInt(bytes.length + 1);
        buffer.writeBytes(bytes);
        buffer.writeByte(BsonConstants.STRING_TERMINATION);
    }

    private void encodeValue(String key, Object value, ByteBuf buffer) throws IOException {
        byte type = determineType(value);
        buffer.writeByte(type);
        encodeCString(key, buffer);
        encodeValue(type, value, buffer);
    }

    private void encodeValue(byte type, Object value, ByteBuf buffer) throws IOException {
        switch (type) {
            case BsonConstants.TYPE_DOUBLE:
                buffer.writeDouble(((Double) value).doubleValue());
                break;
            case BsonConstants.TYPE_UTF8_STRING:
                encodeString(value.toString(), buffer);
                break;
            case BsonConstants.TYPE_EMBEDDED_DOCUMENT:
                encodeDocument((Document) value, buffer);
                break;
            case BsonConstants.TYPE_ARRAY:
                Document document = new Document();
                List<?> array = (List<?>) value;
                for (int i = 0; i < array.size(); i++) {
                    document.put(String.valueOf(i), array.get(i));
                }
                encodeDocument(document, buffer);
                break;
            case BsonConstants.TYPE_DATA:
                if (value instanceof byte[]) {
                    byte[] data = (byte[]) value;
                    buffer.writeInt(data.length);
                    buffer.writeByte(BsonConstants.BINARY_SUBTYPE_GENERIC);
                    buffer.writeBytes(data);
                } else if (value instanceof UUID) {
                    buffer.writeInt(BsonConstants.LENGTH_UUID);
                    buffer.writeByte(BsonConstants.BINARY_SUBTYPE_OLD_UUID);
                    UUID uuid = (UUID) value;
                    buffer.writeLong(uuid.getMostSignificantBits());
                    buffer.writeLong(uuid.getLeastSignificantBits());
                } else {
                    throw new IllegalArgumentException("Unknown data: " + value.getClass());
                }
                break;
            case BsonConstants.TYPE_OBJECT_ID:
                byte[] bytes = ((ObjectId) value).toByteArray();
                if (bytes.length != BsonConstants.LENGTH_OBJECTID) {
                    throw new IllegalArgumentException("Illegal ObjectId: " + value);
                }
                buffer.writeBytes(bytes);
                break;
            case BsonConstants.TYPE_BOOLEAN:
                if (((Boolean) value).booleanValue()) {
                    buffer.writeByte(BsonConstants.BOOLEAN_VALUE_TRUE);
                } else {
                    buffer.writeByte(BsonConstants.BOOLEAN_VALUE_FALSE);
                }
                break;
            case BsonConstants.TYPE_UTC_DATETIME:
                buffer.writeLong(((Date) value).getTime());
                break;
            case BsonConstants.TYPE_REGEX:
                BsonRegularExpression pattern = (BsonRegularExpression) value;
                encodeCString(pattern.getPattern(), buffer);
                encodeCString(pattern.getOptions(), buffer);
                break;
            case BsonConstants.TYPE_INT32:
                buffer.writeInt(((Integer) value).intValue());
                break;
            case BsonConstants.TYPE_TIMESTAMP:
                BSONTimestamp timestamp = (BSONTimestamp) value;
                buffer.writeInt(timestamp.getTime());
                buffer.writeInt(timestamp.getInc());
                break;
            case BsonConstants.TYPE_INT64:
                buffer.writeLong(((Long) value).longValue());
                break;
            case BsonConstants.TYPE_MAX_KEY:
            case BsonConstants.TYPE_MIN_KEY:
            case BsonConstants.TYPE_UNDEFINED:
            case BsonConstants.TYPE_NULL:
                // empty
                break;
            case BsonConstants.TYPE_JAVASCRIPT_CODE:
            case BsonConstants.TYPE_JAVASCRIPT_CODE_WITH_SCOPE:
                throw new IOException("unhandled type: " + value.getClass());
            default:
                throw new IOException("unknown type: " + value.getClass());
        }
    }

    private byte determineType(Object value) {
        if (value == null) {
            return BsonConstants.TYPE_NULL;
        } else if (value instanceof Document) {
            return BsonConstants.TYPE_EMBEDDED_DOCUMENT;
        } else if (value instanceof ObjectId) {
            return BsonConstants.TYPE_OBJECT_ID;
        } else if (value instanceof Integer) {
            return BsonConstants.TYPE_INT32;
        } else if (value instanceof Long) {
            return BsonConstants.TYPE_INT64;
        } else if (value instanceof Double) {
            return BsonConstants.TYPE_DOUBLE;
        } else if (value instanceof String) {
            return BsonConstants.TYPE_UTF8_STRING;
        } else if (value instanceof Boolean) {
            return BsonConstants.TYPE_BOOLEAN;
        } else if (value instanceof byte[]) {
            return BsonConstants.TYPE_DATA;
        } else if (value instanceof List<?>) {
            return BsonConstants.TYPE_ARRAY;
        } else if (value instanceof Date) {
            return BsonConstants.TYPE_UTC_DATETIME;
        } else if (value instanceof BSONTimestamp) {
            return BsonConstants.TYPE_TIMESTAMP;
        } else if (value instanceof BsonRegularExpression) {
            return BsonConstants.TYPE_REGEX;
        } else if (value instanceof MaxKey) {
            return BsonConstants.TYPE_MAX_KEY;
        } else if (value instanceof MinKey) {
            return BsonConstants.TYPE_MIN_KEY;
        } else if (value instanceof UUID) {
            return BsonConstants.TYPE_DATA;
        } else {
            throw new IllegalArgumentException("Unknown type: " + value.getClass());
        }
    }


}
