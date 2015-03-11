package de.bwaldvogel.mongo.backend;

import java.util.Comparator;

import org.bson.BSONObject;

public class DocumentComparator implements Comparator<BSONObject> {

    private ValueComparator valueComparator = new ValueComparator();
    private BSONObject orderBy;

    public DocumentComparator(BSONObject orderBy) {
        if (orderBy == null || orderBy.keySet().isEmpty()) {
            throw new IllegalArgumentException();
        }
        this.orderBy = orderBy;
    }

    public static Object getSubdocumentValue(BSONObject document, String key) {
        int dotPos = key.indexOf('.');
        if (dotPos > 0) {
            String mainKey = key.substring(0, dotPos);
            String subKey = key.substring(dotPos + 1);
            if (subKey.startsWith("$.")) {
                throw new IllegalArgumentException();
            }
            Object subObject = Utils.getFieldValueListSafe(document, mainKey);
            if (subObject instanceof BSONObject) {
                return getSubdocumentValue((BSONObject) subObject, subKey);
            } else {
                return null;
            }
        } else {
            return Utils.getFieldValueListSafe(document, key);
        }
    }

    @Override
    public int compare(BSONObject document1, BSONObject document2) {
        for (String sortKey : orderBy.keySet()) {
            Object value1 = Utils.getSubdocumentValue(document1, sortKey);
            Object value2 = Utils.getSubdocumentValue(document2, sortKey);
            int cmp = valueComparator.compare(value1, value2);
            if (cmp != 0) {
                if (((Number) orderBy.get(sortKey)).intValue() < 0) {
                    cmp = -cmp;
                }
                return cmp;
            }
        }
        return 0;
    }

}
