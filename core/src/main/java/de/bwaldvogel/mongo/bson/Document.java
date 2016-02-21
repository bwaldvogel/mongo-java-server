package de.bwaldvogel.mongo.bson;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class Document implements Map<String, Object>, Bson {

    private static final long serialVersionUID = 1L;

    private final LinkedHashMap<String, Object> documentAsMap;

    public Document() {
        documentAsMap = new LinkedHashMap<>();
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

    @Override
    public void putAll(Map<? extends String, ?> m) {
        documentAsMap.putAll(m);
    }

    @Override
    public Object remove(Object key) {
        return documentAsMap.remove(key);
    }

    @Override
    public Object clone() {
        return documentAsMap.clone();
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
        return documentAsMap.equals(o);
    }

    @Override
    public int hashCode() {
        return documentAsMap.hashCode();
    }

    @Override
    public String toString() {
        return documentAsMap.toString();
    }
}
