package de.bwaldvogel.mongo.backend.aggregation;

import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.bson.Document;

class SumAccumulator implements Accumulator {

    private final String field;
    private final Object expression;

    SumAccumulator(String field, Object expression) {
        this.field = field;
        this.expression = expression;
    }

    @Override
    public void initialize(Document result) {
        result.put(field, getDefault());
    }

    private Object getDefault() {
        if (expression instanceof Integer) {
            return 0;
        } else if (expression instanceof Long) {
            return 0L;
        } else if (expression instanceof Double) {
            return 0.0;
        } else {
            return 0;
        }
    }

    @Override
    public void aggregate(Document result, Document document) {
        Object count = result.get(field);
        result.put(field, add(count, document));
    }

    private Object add(Object count, Document document) {
        return add(count, document, expression);
    }

    private static Object add(Object count, Document document, Object value) {
        if (value instanceof Integer) {
            return ((Number) count).intValue() + ((Integer) value).intValue();
        } else if (value instanceof Long) {
            return ((Number) count).longValue() + ((Long) value).longValue();
        } else if (value instanceof Double) {
            return ((Number) count).doubleValue() + ((Double) value).doubleValue();
        } else if (value instanceof String) {
            if (((String) value).startsWith("$")) {
                String substring = ((String) value).substring(1);
                Object subdocumentValue = Utils.getSubdocumentValue(document, substring);
                return add(count, document, subdocumentValue);
            }
        }
        return ((Number) count).intValue() + 1;
    }
}
