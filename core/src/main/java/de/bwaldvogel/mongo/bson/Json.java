package de.bwaldvogel.mongo.bson;

import java.util.Collection;
import java.util.Date;
import java.util.stream.Collectors;

public final class Json {

    private Json() {
    }

   public static String toJsonValue(Object value) {
        if (value == null) {
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
            return value.toString();
        }
        if (value instanceof Date) {
            return toJsonValue(((Date) value).toInstant().toString());
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
