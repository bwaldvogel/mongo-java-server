package de.bwaldvogel.mongo.backend;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.ObjectId;

public class ValueComparator implements Comparator<Object> {

    private static final List<Class<?>> SORT_PRIORITY = new ArrayList<>();

    static {
        /*
         * http://docs.mongodb.org/manual/faq/developers/#what-is-the-compare-order-for-bson-types
         *
         * Null Numbers (ints, longs, doubles) Symbol, String Object Array
         * BinData ObjectID Boolean Date, Timestamp Regular Expression
         */

        SORT_PRIORITY.add(Number.class);
        SORT_PRIORITY.add(String.class);
        SORT_PRIORITY.add(Document.class);
        SORT_PRIORITY.add(byte[].class);
        SORT_PRIORITY.add(ObjectId.class);
        SORT_PRIORITY.add(Boolean.class);
        SORT_PRIORITY.add(Date.class);
        SORT_PRIORITY.add(Pattern.class);
    }

    @Override
    public int compare(Object value1, Object value2) {

        // also catches null/null case
        if (value1 == value2)
            return 0;

        int t1 = getTypeOrder(value1);
        int t2 = getTypeOrder(value2);
        if (t1 != t2) {
            return t1 < t2 ? -1 : +1;
        }

        Class<?> clazz = value1.getClass();

        if (ObjectId.class.isAssignableFrom(clazz)) {
            return ((ObjectId) value1).compareTo((ObjectId) value2);
        }

        if (Number.class.isAssignableFrom(clazz)) {
            return Double.compare(((Number) value1).doubleValue(), ((Number) value2).doubleValue());
        }

        if (String.class.isAssignableFrom(clazz)) {
            return value1.toString().compareTo(value2.toString());
        }

        if (Date.class.isAssignableFrom(clazz)) {
            Date date1 = (Date) value1;
            Date date2 = (Date) value2;
            return date1.compareTo(date2);
        }

        if (Boolean.class.isAssignableFrom(clazz)) {
            boolean b1 = ((Boolean) value1).booleanValue();
            boolean b2 = ((Boolean) value2).booleanValue();
            return (!b1 && b2) ? -1 : (b1 && !b2) ? +1 : 0;
        }

        if (Document.class.isAssignableFrom(clazz)) {
            for (String key : ((Document) value1).keySet()) {
                int cmp = compare(((Document) value1).get(key), ((Document) value2).get(key));
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        }

        throw new UnsupportedOperationException("can't compare " + clazz);

    }

    private int getTypeOrder(Object obj) {
        if (obj == null)
            return -1;
        for (int idx = 0; idx < SORT_PRIORITY.size(); idx++) {
            if (SORT_PRIORITY.get(idx).isAssignableFrom(obj.getClass())) {
                return idx;
            }
        }
        throw new UnsupportedOperationException("can't sort " + obj.getClass());
    }
}
