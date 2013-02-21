package de.bwaldvogel.mongo.backend;

public abstract class MongoCollection {

    public static Object normalizeValue(Object value) {
        if (value == null)
            return null;
        if (value instanceof Number) {
            return Double.valueOf(((Number) value).doubleValue());
        } else {
            return value;
        }
    }

    public static boolean nullAwareEquals(Object newValue, Object oldValue) {
        if (newValue == oldValue)
            return true;

        if (newValue != null) {
            return normalizeValue(newValue).equals(normalizeValue(oldValue));
        }

        return newValue == oldValue;
    }

}
