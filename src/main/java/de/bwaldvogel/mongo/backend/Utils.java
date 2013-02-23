package de.bwaldvogel.mongo.backend;

import java.util.List;

import org.bson.BSONObject;

import com.mongodb.DefaultDBEncoder;

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

    public static boolean isFieldTrue(BSONObject document, String field) {
        Object value = document.get(field);
        if (value == null)
            return false;

        if (value.equals(Boolean.TRUE)) {
            return true;
        }
        return normalizeValue(value).equals(Double.valueOf(1.0));
    }

    public static Object normalizeValue(Object value) {
        if (value == null)
            return null;
        if (value instanceof Number) {
            return Double.valueOf(((Number) value).doubleValue());
        } else {
            return value;
        }
    }

    public static boolean nullAwareEquals(Object a, Object b) {
        if (a == b)
            return true;

        if (a != null) {
            return normalizeValue(a).equals(normalizeValue(b));
        }

        return a == b;
    }

    public static long calculateSize(BSONObject document) {
        return new DefaultDBEncoder().encode(document).length;
    }

    @SuppressWarnings("unchecked")
    public static List<Object> asList(Object value) {
        return (List<Object>) value;
    }

    public static boolean containsQueryExpression(Object value) {
        if (value == null)
            return false;

        if (!(value instanceof BSONObject)) {
            return false;
        }

        BSONObject doc = (BSONObject) value;
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

}
