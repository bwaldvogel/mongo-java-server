package de.bwaldvogel.mongo.backend.memory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

import org.bson.BSONObject;
import org.bson.types.ObjectId;

public class BSONComparator implements Comparator<BSONObject> {

    private static final List<Class<?>> SORT_PRIORITY = new ArrayList<Class<?>>();

    static {
        /*
         * http://docs.mongodb.org/manual/faq/developers/#what-is-the-compare-order
         * -for-bson-types
         *
         * Null Numbers (ints, longs, doubles) Symbol, String Object Array
         * BinData ObjectID Boolean Date, Timestamp Regular Expression
         */

        SORT_PRIORITY.add(Number.class);
        SORT_PRIORITY.add(String.class);
        SORT_PRIORITY.add(BSONObject.class);
        SORT_PRIORITY.add(byte[].class);
        SORT_PRIORITY.add(ObjectId.class);
        SORT_PRIORITY.add(Boolean.class);
        SORT_PRIORITY.add(Date.class);
        SORT_PRIORITY.add(Pattern.class);
    }

    private BSONObject orderBy;

    public BSONComparator(BSONObject orderBy) {
        if (orderBy == null || orderBy.keySet().isEmpty()) {
            throw new IllegalArgumentException();
        }
        if (orderBy.keySet().size() > 1) {
            throw new IllegalArgumentException();
        }

        this.orderBy = orderBy;
    }

    @Override
    public int compare(BSONObject o1, BSONObject o2) {
        for (String sortKey : orderBy.keySet()) {
            Object a1 = o1.get(sortKey);
            Object a2 = o2.get(sortKey);
            int cmp = compareObs(a1, a2);
            if (cmp != 0) {
                if (((Number) orderBy.get(sortKey)).intValue() < 0) {
                    cmp = -cmp;
                }
                return cmp;
            }
        }
        return 0;
    }

    private int compareObs(Object a1, Object a2) {
        int t1 = getTypeOrder(a1);
        int t2 = getTypeOrder(a2);
        if (t1 != t2) {
            return t1 < t2 ? -1 : +1;
        }

        Class<?> clazz = a1.getClass();

        if (Number.class.isAssignableFrom(clazz)) {
            return Double.compare(((Number) a1).doubleValue(), ((Number) a2).doubleValue());
        }

        if (String.class.isAssignableFrom(clazz)) {
            return a1.toString().compareTo(a2.toString());
        }

        if (Date.class.isAssignableFrom(clazz)) {
            return a1.toString().compareTo(a2.toString());
        }

        if (Boolean.class.isAssignableFrom(clazz)) {
            boolean b1 = ((Boolean) a1).booleanValue();
            boolean b2 = ((Boolean) a2).booleanValue();
            return (!b1 && b2) ? -1 : (b1 && !b2) ? +1 : 0;
        }
        if (BSONObject.class.isAssignableFrom(clazz)) {
            for (String key : ((BSONObject) a1).keySet()) {
                int cmp = compareObs(((BSONObject) a1).get(key), ((BSONObject) a2).get(key));
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
