package de.bwaldvogel.mongo.bson;

import java.util.Collection;
import java.util.Date;
import java.util.stream.Collectors;

import de.bwaldvogel.mongo.backend.Missing;

public final class Json {

    private Json() {
    }

    public static String toJsonValue(Object value) {
        return toJsonValue(value, false, "{", "}");
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
        if (value instanceof String) {
            return "\"" + escapeJson((String) value) + "\"";
        }
        if (value instanceof Document) {
            Document document = (Document) value;
            return document.toString(compactKey, jsonPrefix, jsonSuffix);
        }
        if (value instanceof Date) {
            Date date = (Date) value;
            return toJsonValue(date.toInstant().toString());
        }
        if (value instanceof Collection) {
            Collection<?> collection = (Collection<?>) value;
            return collection.stream()
                .map(Json::toJsonValue)
                .collect(Collectors.joining(", ", "[", "]"));
        }
        if (value instanceof ObjectId) {
            ObjectId objectId = (ObjectId) value;
            return objectId.getHexData();
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
}
