package de.bwaldvogel.mongo.wire.bson;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import de.bwaldvogel.mongo.backend.Missing;
import de.bwaldvogel.mongo.bson.BinData;
import de.bwaldvogel.mongo.bson.BsonJavaScript;
import de.bwaldvogel.mongo.bson.BsonRegularExpression;
import de.bwaldvogel.mongo.bson.BsonTimestamp;
import de.bwaldvogel.mongo.bson.Decimal128;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.LegacyUUID;
import de.bwaldvogel.mongo.bson.MaxKey;
import de.bwaldvogel.mongo.bson.MinKey;
import de.bwaldvogel.mongo.bson.ObjectId;
import de.bwaldvogel.mongo.wire.BsonConstants;
import io.netty.buffer.ByteBuf;

public class BsonEncoder {

    private BsonEncoder() {
    }

    public static void encodeDocument(Map<String, ?> document, ByteBuf out) {
        int indexBefore = out.writerIndex();
        out.writeIntLE(0); // total number of bytes will be written later

        for (String key : document.keySet()) {
            encodeValue(key, document.get(key), out);
        }

        out.writeByte(BsonConstants.TERMINATING_BYTE);
        int indexAfter = out.writerIndex();
        out.writerIndex(indexBefore);
        out.writeIntLE(indexAfter - indexBefore);
        out.writerIndex(indexAfter);
    }

    private static void encodeCString(String data, ByteBuf buffer) {
        byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        buffer.writeBytes(bytes);
        buffer.writeByte(BsonConstants.STRING_TERMINATION);
    }

    private static void encodeString(String data, ByteBuf buffer) {
        byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        buffer.writeIntLE(bytes.length + 1);
        buffer.writeBytes(bytes);
        buffer.writeByte(BsonConstants.STRING_TERMINATION);
    }

    private static void encodeValue(String key, Object value, ByteBuf buffer) {
        byte type = determineType(value);
        buffer.writeByte(type);
        encodeCString(key, buffer);
        encodeValue(type, value, buffer);
    }

    public static void encodeValue(byte type, Object value, ByteBuf buffer) {
        switch (type) {
            case BsonConstants.TYPE_DOUBLE ->
                buffer.writeLongLE(Double.doubleToRawLongBits(((Double) value).doubleValue()));
            case BsonConstants.TYPE_UTF8_STRING -> encodeString(value.toString(), buffer);
            case BsonConstants.TYPE_EMBEDDED_DOCUMENT -> {
                @SuppressWarnings("unchecked")
                Map<String, ?> valueAsMap = (Map<String, ?>) value;
                encodeDocument(valueAsMap, buffer);
            }
            case BsonConstants.TYPE_ARRAY -> {
                Document document = new Document();
                List<?> array = collectionToList(value);
                for (int i = 0; i < array.size(); i++) {
                    document.put(String.valueOf(i), array.get(i));
                }
                encodeDocument(document, buffer);
            }
            case BsonConstants.TYPE_DATA -> {
                switch (value) {
                    case BinData binData -> {
                        byte[] data = binData.getData();
                        buffer.writeIntLE(data.length);
                        buffer.writeByte(BsonConstants.BINARY_SUBTYPE_GENERIC);
                        buffer.writeBytes(data);
                    }
                    case UUID uuid -> {
                        buffer.writeIntLE(BsonConstants.LENGTH_UUID);
                        buffer.writeByte(BsonConstants.BINARY_SUBTYPE_UUID);
                        buffer.writeLong(uuid.getMostSignificantBits());
                        buffer.writeLong(uuid.getLeastSignificantBits());
                    }
                    case LegacyUUID legacyUUID -> {
                        UUID uuid = legacyUUID.getUuid();
                        buffer.writeIntLE(BsonConstants.LENGTH_UUID);
                        buffer.writeByte(BsonConstants.BINARY_SUBTYPE_OLD_UUID);
                        buffer.writeLongLE(uuid.getMostSignificantBits());
                        buffer.writeLongLE(uuid.getLeastSignificantBits());
                    }
                    default -> throw new IllegalArgumentException("Unknown data: " + value.getClass());
                }
            }
            case BsonConstants.TYPE_OBJECT_ID -> {
                byte[] bytes = ((ObjectId) value).toByteArray();
                if (bytes.length != BsonConstants.LENGTH_OBJECTID) {
                    throw new IllegalArgumentException("Illegal ObjectId: " + value);
                }
                buffer.writeBytes(bytes);
            }
            case BsonConstants.TYPE_BOOLEAN -> {
                if (((Boolean) value).booleanValue()) {
                    buffer.writeByte(BsonConstants.BOOLEAN_VALUE_TRUE);
                } else {
                    buffer.writeByte(BsonConstants.BOOLEAN_VALUE_FALSE);
                }
            }
            case BsonConstants.TYPE_UTC_DATETIME -> {
                Instant instant = (Instant) value;
                buffer.writeLongLE(instant.toEpochMilli());
            }
            case BsonConstants.TYPE_REGEX -> {
                BsonRegularExpression pattern = (BsonRegularExpression) value;
                encodeCString(pattern.getPattern(), buffer);
                encodeCString(pattern.getOptions(), buffer);
            }
            case BsonConstants.TYPE_INT32 -> buffer.writeIntLE(((Integer) value).intValue());
            case BsonConstants.TYPE_TIMESTAMP -> {
                BsonTimestamp timestamp = (BsonTimestamp) value;
                buffer.writeLongLE(timestamp.getValue());
            }
            case BsonConstants.TYPE_INT64 -> buffer.writeLongLE(((Long) value).longValue());
            case BsonConstants.TYPE_DECIMAL128 -> {
                Decimal128 decimal128 = (Decimal128) value;
                buffer.writeLongLE(decimal128.getLow());
                buffer.writeLongLE(decimal128.getHigh());
            }
            case BsonConstants.TYPE_MAX_KEY, BsonConstants.TYPE_MIN_KEY,
                 BsonConstants.TYPE_UNDEFINED, BsonConstants.TYPE_NULL -> {
                // empty
            }
            case BsonConstants.TYPE_JAVASCRIPT_CODE -> {
                BsonJavaScript javaScript = (BsonJavaScript) value;
                encodeString(javaScript.getCode(), buffer);
            }
            case BsonConstants.TYPE_JAVASCRIPT_CODE_WITH_SCOPE ->
                throw new IllegalArgumentException("unhandled type: " + value.getClass());
            default -> throw new IllegalArgumentException("unknown type: " + value.getClass());
        }
    }

    public static byte determineType(Object value) {
        return switch (value) {
            case null -> BsonConstants.TYPE_NULL;
            case Map<?, ?> map -> BsonConstants.TYPE_EMBEDDED_DOCUMENT;
            case ObjectId objectId -> BsonConstants.TYPE_OBJECT_ID;
            case Integer integer -> BsonConstants.TYPE_INT32;
            case Long longValue -> BsonConstants.TYPE_INT64;
            case Decimal128 decimal128 -> BsonConstants.TYPE_DECIMAL128;
            case Double doubleValue -> BsonConstants.TYPE_DOUBLE;
            case String string -> BsonConstants.TYPE_UTF8_STRING;
            case Boolean booleanValue -> BsonConstants.TYPE_BOOLEAN;
            case BinData binData -> BsonConstants.TYPE_DATA;
            case UUID uuid -> BsonConstants.TYPE_DATA;
            case LegacyUUID legacyUUID -> BsonConstants.TYPE_DATA;
            case Collection<?> collection -> BsonConstants.TYPE_ARRAY;
            case Instant instant -> BsonConstants.TYPE_UTC_DATETIME;
            case BsonTimestamp timestamp -> BsonConstants.TYPE_TIMESTAMP;
            case BsonRegularExpression pattern -> BsonConstants.TYPE_REGEX;
            case MaxKey maxKey -> BsonConstants.TYPE_MAX_KEY;
            case MinKey minKey -> BsonConstants.TYPE_MIN_KEY;
            case BsonJavaScript javaScript -> BsonConstants.TYPE_JAVASCRIPT_CODE;
            case Missing missing -> throw new IllegalArgumentException(
                "Unexpected missing value. This must not happen. Please report a bug.");
            default -> throw new IllegalArgumentException("Unknown type: " + value.getClass());
        };
    }

    private static List<?> collectionToList(Object value) {
        if (value instanceof List<?> list) {
            return list;
        } else {
            return new ArrayList<>((Collection<?>) value);
        }
    }

}
