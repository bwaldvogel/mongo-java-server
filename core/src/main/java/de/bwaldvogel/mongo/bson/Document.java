package de.bwaldvogel.mongo.bson;

import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public final class Document implements Map<String, Object>, Bson {

    private static final long serialVersionUID = 1L;

    private final LinkedHashMap<String, Object> documentAsMap = new LinkedHashMap<>();

    public Document() {
    }

    public Document(String key, Object value) {
        this();
        append(key, value);
    }

    public Document(Map<String, Object> map) {
        this();
        putAll(map);
    }

    public Document append(String key, Object value) {
        put(key, value);
        return this;
    }

    public Document appendAll(Map<String, Object> map) {
        putAll(map);
        return this;
    }

    @Override
    public boolean containsValue(Object value) {
        return documentAsMap.containsValue(value);
    }

    @Override
    public Object get(Object key) {
        return documentAsMap.get(key);
    }

    @Override
    public void clear() {
        documentAsMap.clear();
    }

    @Override
    public int size() {
        return documentAsMap.size();
    }

    @Override
    public boolean isEmpty() {
        return documentAsMap.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return documentAsMap.containsKey(key);
    }

    @Override
    public Object put(String key, Object value) {
        return documentAsMap.put(key, value);
    }

    public void putIfNotNull(String key, Object value) {
        if (value != null) {
            put(key, value);
        }
    }

    @Override
    public void putAll(Map<? extends String, ?> m) {
        documentAsMap.putAll(m);
    }

    @Override
    public Object remove(Object key) {
        return documentAsMap.remove(key);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object clone() {
        return new Document((Map<String, Object>) documentAsMap.clone());
    }

    @Override
    public Set<String> keySet() {
        return documentAsMap.keySet();
    }

    @Override
    public Collection<Object> values() {
        return documentAsMap.values();
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
        return documentAsMap.entrySet();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (o == this) {
            return true;
        }
        if (!(o instanceof Document)) {
            return false;
        }
        return documentAsMap.equals(o);
    }

    @Override
    public int hashCode() {
        return documentAsMap.hashCode();
    }

    @Override
    public String toString() {
        return documentAsMap.entrySet().stream()
            .map(entry -> "\"" + escapeJson(entry.getKey()) + "\" : " + toJsonValue(entry.getValue()))
            .collect(Collectors.joining(", ", "{", "}"));
    }

    static String toJsonValue(Object value) {
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
                .map(Document::toJsonValue)
                .collect(Collectors.joining(", ", "[", "]"));
        }
        throw new IllegalArgumentException("Unknown value: " + value);
    }

    private static String escapeJson(String input) {
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
