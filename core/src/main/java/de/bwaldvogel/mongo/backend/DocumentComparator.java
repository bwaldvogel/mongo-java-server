package de.bwaldvogel.mongo.backend;

import java.util.Comparator;

import de.bwaldvogel.mongo.bson.Document;

public class DocumentComparator implements Comparator<Document> {

    private ValueComparator valueComparator = new ValueComparator();
    private Document orderBy;

    public DocumentComparator(Document orderBy) {
        if (orderBy == null || orderBy.keySet().isEmpty()) {
            throw new IllegalArgumentException();
        }
        this.orderBy = orderBy;
    }

    @Override
    public int compare(Document document1, Document document2) {
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
