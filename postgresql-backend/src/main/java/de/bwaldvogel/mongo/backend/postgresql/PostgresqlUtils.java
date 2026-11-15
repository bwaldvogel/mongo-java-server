package de.bwaldvogel.mongo.backend.postgresql;

import static de.bwaldvogel.mongo.backend.postgresql.JsonConverter.toJson;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import de.bwaldvogel.mongo.backend.Missing;
import de.bwaldvogel.mongo.bson.Document;

public final class PostgresqlUtils {

    private static final String SQL_ERROR_DUPLICATE_KEY = "23505";

    private PostgresqlUtils() {
    }

    public static String toDataKey(String key) {
        if (!key.matches("^[a-zA-Z0-9_.]+$")) {
            throw new IllegalArgumentException("Illegal key: " + key);
        }
        List<String> keys = List.of(key.split("\\."));
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

    static String toNormalizedDataKey(String key) {
        String dataKey = toDataKey(key);
        return "CASE WHEN (" + dataKey + ")::numeric = -0.0 THEN '0' ELSE " + dataKey + " END";
    }

    public static String toQueryValue(Object queryValue) {
        Objects.requireNonNull(queryValue);
        return switch (queryValue) {
            case String string -> string;
            case Number number -> {
                if (number.doubleValue() == -0.0) {
                    yield "0";
                }
                String numberString = number.toString();
                yield numberString.replaceAll("^(\\d+)\\.0+$", "$1");
            }
            case Document document -> toJsonWithClass(document);
            case Map<?, ?> map -> toJson(map);
            case List<?> list -> {
                List<?> values = list.stream()
                    .map(value -> value instanceof Missing ? null : value)
                    .collect(Collectors.toList());
                yield toJson(values);
            }
            default -> toJsonWithClass(queryValue);
        };
    }

    private static String toJsonWithClass(Object queryValue) {
        String valueAsJson = toJson(queryValue);
        return valueAsJson.replaceFirst("\\{", "\\{\"@class\":\"" + queryValue.getClass().getName() + "\",");
    }

    public static boolean isErrorDuplicateKey(SQLException e) {
        return e.getSQLState().equals(SQL_ERROR_DUPLICATE_KEY);
    }

}
