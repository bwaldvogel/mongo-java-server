package de.bwaldvogel.mongo.bson;

import java.time.Instant;
import java.util.Collection;
import java.util.HexFormat;
import java.util.UUID;
import java.util.stream.Collectors;

import de.bwaldvogel.mongo.backend.Missing;

public final class Json {

    private Json() {
    }

    public static String toJsonValue(Object value) {
        return toJsonValue(value, false, "{", "}");
    }

    public static String toCompactJsonValue(Object value) {
        return toJsonValue(value, true, "{ ", " }");
    }

    public static String toJsonValue(Object value, boolean compactKey, String jsonPrefix, String jsonSuffix) {
        if (Missing.isNullOrMissing(value)) {
            return "null";
        }
        if (value instanceof Number) {
            return value.toString();
        }
        if (value instanceof Boolean) {
            return value.toString();
        }
        if (value instanceof String string) {
            return "\"" + escapeJson(string) + "\"";
        }
        if (value instanceof Document document) {
            return document.toString(compactKey, jsonPrefix, jsonSuffix);
        }
        if (value instanceof Instant instant) {
            return toJsonValue(instant.toString());
        }
        if (value instanceof Collection<?> collection) {
            if (collection.isEmpty()) {
                return "[]";
            }
            return collection.stream()
                .map(v -> toJsonValue(v, compactKey, "{ ", " }"))
                .collect(Collectors.joining(", ", "[ ", " ]"));
        }
        if (value instanceof ObjectId objectId) {
            return objectId.getHexData();
        }
        if (value instanceof BinData binData) {
            return "BinData(0, " + toHex(binData.getData()) + ")";
        }
        if (value instanceof LegacyUUID legacyUUID) {
            UUID uuid = legacyUUID.getUuid();
            return "BinData(3, " + toHex(uuid) + ")";
        }
        if (value instanceof UUID uuid) {
            return "UUID(\"" + uuid + "\")";
        }
        return toJsonValue(value.toString());
    }

    static String escapeJson(String input) {
        String escaped = input;
        escaped = escaped.replace("\\", "\\\\");
        escaped = escaped.replace("\"", "\\\"");
        escaped = escaped.replace("\b", "\\b");
        escaped = escaped.replace("\f", "\\f");
        escaped = escaped.replace("\n", "\\n");
        escaped = escaped.replace("\r", "\\r");
        escaped = escaped.replace("\t", "\\t");
        return escaped;
    }

    private static String toHex(UUID uuid) {
        return toLittleEndianHex(uuid.getMostSignificantBits())
            + toLittleEndianHex(uuid.getLeastSignificantBits());
    }

    private static String toLittleEndianHex(long value) {
        return HexFormat.of().withUpperCase().toHexDigits(Long.reverseBytes(value));
    }

    private static String toHex(byte[] bytes) {
        return HexFormat.of().withUpperCase().formatHex(bytes);
    }

}
