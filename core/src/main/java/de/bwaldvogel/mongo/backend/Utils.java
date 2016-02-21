package de.bwaldvogel.mongo.backend;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.exception.MongoServerException;
import de.bwaldvogel.mongo.wire.BsonEncoder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class Utils {

    public static Number addNumbers(Number a, Number b) {
        if (a instanceof Double || b instanceof Double) {
            return Double.valueOf(a.doubleValue() + b.doubleValue());
        } else if (a instanceof Float || b instanceof Float) {
            return Float.valueOf(a.floatValue() + b.floatValue());
        } else if (a instanceof Long || b instanceof Long) {
            return Long.valueOf(a.longValue() + b.longValue());
        } else if (a instanceof Integer || b instanceof Integer) {
            return Integer.valueOf(a.intValue() + b.intValue());
        } else if (a instanceof Short || b instanceof Short) {
            return Short.valueOf((short) (a.shortValue() + b.shortValue()));
        } else {
            throw new UnsupportedOperationException("can not add " + a + " and " + b);
        }
    }

    public static Number multiplyNumbers(Number a, Number b) {
        if (a instanceof Double || b instanceof Double) {
            return Double.valueOf(a.doubleValue() * b.doubleValue());
        } else if (a instanceof Float || b instanceof Float) {
            return Float.valueOf(a.floatValue() * b.floatValue());
        } else if (a instanceof Long || b instanceof Long) {
            return Long.valueOf(a.longValue() * b.longValue());
        } else if (a instanceof Integer || b instanceof Integer) {
            return Integer.valueOf(a.intValue() * b.intValue());
        } else if (a instanceof Short || b instanceof Short) {
            return Short.valueOf((short) (a.shortValue() * b.shortValue()));
        } else {
            throw new UnsupportedOperationException("can not multiply " + a + " and " + b);
        }
    }

    public static Object getSubdocumentValue(Document document, String key) {
        int dotPos = key.indexOf('.');
        if (dotPos > 0) {
            String mainKey = key.substring(0, dotPos);
            String subKey = key.substring(dotPos + 1);
            if (subKey.startsWith("$.")) {
                throw new IllegalArgumentException();
            }
            Object subObject = Utils.getFieldValueListSafe(document, mainKey);
            if (subObject instanceof Document) {
                return getSubdocumentValue((Document) subObject, subKey);
            } else {
                return null;
            }
        } else {
            return Utils.getFieldValueListSafe(document, key);
        }
    }

    public static String getDatabaseNameFromFullName(String fullName) {
        int dotPos = fullName.indexOf('.');
        return fullName.substring(0, dotPos);
    }

    public static String getCollectionNameFromFullName(String fullName) {
        int dotPos = fullName.indexOf('.');
        return fullName.substring(dotPos + 1);
    }

    public static boolean isTrue(Object value) {
        if (value == null) {
            return false;
        }

        if (value instanceof Boolean) {
            return ((Boolean) value).booleanValue();
        }

        if (value instanceof Number) {
            return ((Number) value).doubleValue() != 0.0;
        }

        return true;
    }

    public static Object normalizeValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return Double.valueOf(((Number) value).doubleValue());
        } else {
            return value;
        }
    }

    public static boolean nullAwareEquals(Object a, Object b) {
        if (a == b) {
            return true;
        } else if (a == null || b == null) {
            return (a == b);
        } else if (a instanceof byte[] && b instanceof byte[]) {
            byte[] bytesA = (byte[]) a;
            byte[] bytesB = (byte[]) b;
            return Arrays.equals(bytesA, bytesB);
        } else {
            Object normalizedA = normalizeValue(a);
            Object normalizedB = normalizeValue(b);
            return normalizedA.equals(normalizedB);
        }
    }

    public static long calculateSize(Document document) throws MongoServerException {
        ByteBuf buffer = Unpooled.buffer();
        try {
            new BsonEncoder().encodeDocument(document, buffer);
            return buffer.writerIndex();
        } catch (IOException e) {
            throw new MongoServerException("Failed to calculate document size", e);
        } finally {
            buffer.release();
        }
    }

    public static boolean containsQueryExpression(Object value) {
        if (value == null) {
            return false;
        }

        if (!(value instanceof Document)) {
            return false;
        }

        Document doc = (Document) value;
        for (String key : doc.keySet()) {
            if (key.startsWith("$")) {
                return true;
            }
            if (containsQueryExpression(doc.get(key))) {
                return true;
            }
        }
        return false;
    }

    public static Object getFieldValueListSafe(Object document, String field) throws IllegalArgumentException {
        if (document == null) {
            return null;
        }

        if (field.equals("$") || field.contains(".")) {
            throw new IllegalArgumentException("illegal field: " + field);
        }

        if (document instanceof List<?>) {
            if (field.matches("\\d+")) {
                int pos = Integer.parseInt(field);
                List<?> list = (List<?>) document;
                if (pos >= 0 && pos < list.size()) {
                    return list.get(pos);
                } else {
                    return null;
                }
            } else {
                throw new IllegalArgumentException("illegal field: " + field);
            }
        } else if (document instanceof Document) {
            return ((Document) document).get(field);
        }

        throw new IllegalArgumentException("illegal document: " + document);
    }

    public static boolean hasSubdocumentValue(Object document, String key) throws MongoServerError {
        int dotPos = key.indexOf('.');
        if (dotPos > 0) {
            String mainKey = key.substring(0, dotPos);
            String subKey = getSubkey(key, dotPos, new AtomicReference<Integer>());
            Object subObject = Utils.getFieldValueListSafe(document, mainKey);
            if (subObject instanceof Document || subObject instanceof List<?>) {
                return hasSubdocumentValue(subObject, subKey);
            } else {
                return false;
            }
        } else {
            return Utils.hasFieldValueListSafe(document, key);
        }
    }

    public static String getSubkey(String key, int dotPos, AtomicReference<Integer> matchPos) throws MongoServerError {
        String subKey = key.substring(dotPos + 1);

        if (subKey.matches("\\$(\\..+)?")) {
            if (matchPos == null || matchPos.get() == null) {
                throw new MongoServerError(16650, //
                        "Cannot apply the positional operator without a corresponding query " //
                                + "field containing an array.");
            }
            Integer pos = matchPos.getAndSet(null);
            return subKey.replaceFirst("\\$", String.valueOf(pos));
        }
        return subKey;
    }

    static boolean hasFieldValueListSafe(Object document, String field) throws IllegalArgumentException {
        if (document == null) {
            return false;
        }

        if (field.equals("$") || field.contains(".")) {
            throw new IllegalArgumentException("illegal field: " + field);
        }

        if (document instanceof List<?>) {
            if (field.matches("\\d+")) {
                int pos = Integer.parseInt(field);
                List<?> list = (List<?>) document;
                return (pos >= 0 && pos < list.size());
            } else {
                throw new IllegalArgumentException("illegal field: " + field);
            }
        } else if (document instanceof Document) {
            return ((Document) document).containsKey(field);
        }

        throw new IllegalArgumentException("illegal document: " + document);
    }

    public static void markOkay(Document result) {
        result.put("ok", Integer.valueOf(1));
    }

    public static void setListSafe(Object document, String key, Object obj) {
        if (document instanceof List<?>) {
            int pos = Integer.parseInt(key);
            @SuppressWarnings("unchecked")
            List<Object> list = ((List<Object>) document);
            while (list.size() <= pos) {
                list.add(null);
            }
            list.set(pos, obj);
        } else {
            ((Document) document).put(key, obj);
        }
    }

    public static Object removeListSafe(Object document, String key) {
        if (document instanceof Document) {
            return ((Document) document).remove(key);
        } else if (document instanceof List<?>) {
            int pos = Integer.parseInt(key);
            @SuppressWarnings("unchecked")
            List<Object> list = ((List<Object>) document);
            if (list.size() > pos) {
                return list.set(pos, null);
            } else {
                return null;
            }
        } else {
            throw new IllegalArgumentException();
        }
    }

    public static String join(List<Integer> array, char c) {
        final StringBuilder sb = new StringBuilder();
        for (int value : array) {
            if (sb.length() > 0) {
                sb.append(c);
            }
            sb.append(Integer.toString(value));
        }
        return sb.toString();
    }

}
