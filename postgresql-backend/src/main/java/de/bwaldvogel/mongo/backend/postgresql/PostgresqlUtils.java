package de.bwaldvogel.mongo.backend.postgresql;

import static de.bwaldvogel.mongo.backend.postgresql.JsonConverter.toJson;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import de.bwaldvogel.mongo.bson.Document;

public final class PostgresqlUtils {

    private PostgresqlUtils() {
    }

    public static String toDataKey(String key) {
        if (!key.matches("^[a-zA-Z0-9_.]+$")) {
            throw new IllegalArgumentException("Illegal key: " + key);
        }
        List<String> keys = Arrays.asList(key.split("\\."));
        StringBuilder sb = new StringBuilder("data");
        for (int i = 0; i < keys.size(); i++) {
            if (i == keys.size() - 1) {
                sb.append(" ->> ");
            } else {
                sb.append(" -> ");
            }
            sb.append("'").append(keys.get(i)).append("'");
        }
        return sb.toString();
    }

    public static String toQueryValue(Object queryValue) throws IOException {
        Objects.requireNonNull(queryValue);
        if (queryValue instanceof String) {
            return (String) queryValue;
        } else if (queryValue instanceof Number) {
            return queryValue.toString();
        } else if (queryValue instanceof Document) {
            return toJsonWithClass(queryValue);
        } else if (queryValue instanceof Map) {
            return toJson(queryValue);
        } else if (queryValue instanceof List) {
            return toJson(queryValue);
        } else {
            return toJsonWithClass(queryValue);
        }
    }

    private static String toJsonWithClass(Object queryValue) throws IOException {
        String valueAsJson = toJson(queryValue);
        return valueAsJson.replaceFirst("\\{", "\\{\"@class\":\"" + queryValue.getClass().getName() + "\",");
    }
}
