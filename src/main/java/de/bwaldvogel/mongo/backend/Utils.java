package de.bwaldvogel.mongo.backend;

import java.util.List;
import java.util.regex.Pattern;

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

    public static Object getSubdocumentValue(BSONObject document, String key) {
        int dotPos = key.indexOf('.');
        if (dotPos > 0) {
            String mainKey = key.substring(0, dotPos);
            String subKey = key.substring(dotPos + 1);
            Object subObject = Utils.getListSafe(document, mainKey);
            if (subObject instanceof BSONObject) {
                return getSubdocumentValue((BSONObject) subObject, subKey);
            } else {
                return null;
            }
        } else {
            return Utils.getListSafe(document, key);
        }
    }

    public static boolean isTrue(Object value) {
        if (value == null)
            return false;

        if (value instanceof Boolean) {
            return ((Boolean) value).booleanValue();
        }

        if (value instanceof Number) {
            return ((Number) value).doubleValue() != 0.0;
        }

        return true;
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

    public static Object getListSafe(Object document, String field) {
        if (document == null) {
            return null;
        }

        if (field.contains("."))
            throw new IllegalArgumentException("illegal field: " + field);

        if (document instanceof BSONObject) {
            return ((BSONObject) document).get(field);
        }

        if (document instanceof List<?>) {
            if (field.matches("\\d+")) {
                int pos = Integer.parseInt(field);
                List<?> list = (List<?>) document;
                if (pos >= 0 && pos < list.size()) {
                    return list.get(pos);
                }
            }
        }
        return null;
    }

    public static void markOkay(BSONObject result) {
        result.put("ok", Integer.valueOf(1));
    }

    public static Pattern createPattern(String regex, String options) {
        int flags = 0;
        for (char flag : options.toCharArray()) {
            switch (flag) {
            case 'i':
                flags |= Pattern.CASE_INSENSITIVE;
                break;
            case 'm':
                flags |= Pattern.MULTILINE;
                break;
            case 'x':
                flags |= Pattern.COMMENTS;
                break;
            case 's':
                flags |= Pattern.DOTALL;
                break;
            case 'u':
                flags |= Pattern.UNICODE_CASE;
                break;
            default:
                throw new IllegalArgumentException("unknown pattern flag: '" + flag + "'");
            }

        }

        // always enable unicode aware case matching
        flags |= Pattern.UNICODE_CASE;

        return Pattern.compile(regex, flags);
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
            ((BSONObject) document).put(key, obj);
        }
    }

    public static void removeListSafe(Object document, String key) {
        if (document instanceof BSONObject) {
            ((BSONObject) document).removeField(key);
        } else if (document instanceof List<?>) {
            int pos = Integer.parseInt(key);
            @SuppressWarnings("unchecked")
            List<Object> list = ((List<Object>) document);
            if (list.size() > pos) {
                list.set(pos, null);
            }
        } else {
            throw new IllegalArgumentException();
        }
    }

}
