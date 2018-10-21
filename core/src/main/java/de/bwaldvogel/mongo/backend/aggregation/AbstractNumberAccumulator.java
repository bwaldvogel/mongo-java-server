package de.bwaldvogel.mongo.backend.aggregation;

import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.bson.Document;

abstract class AbstractNumberAccumulator implements Accumulator {

    private final String field;
    private final Object expression;

    AbstractNumberAccumulator(String field, Object expression) {
        this.field = field;
        this.expression = expression;
    }

    @Override
    public void initialize(Document result) {
        initialize(result, field, expression);
    }

    protected void initialize(Document result, String field, Object expression) {
    }

    @Override
    public void aggregate(Document result, Document document) {
        Object value = result.get(field);
        result.put(field, calculate(value, document, expression));
    }

    private Object calculate(Object aggregatedValue, Document document, Object value) {
        if (value instanceof Integer) {
            return calculate(((Integer) aggregatedValue), ((Integer) value).intValue());
        } else if (value instanceof Long) {
            return calculate(((Long) aggregatedValue), ((Long) value).longValue());
        } else if (value instanceof Double) {
            return calculate(((Double) aggregatedValue), ((Double) value).doubleValue());
        } else if (value instanceof String) {
            if (((String) value).startsWith("$")) {
                String substring = ((String) value).substring(1);
                Object subdocumentValue = Utils.getSubdocumentValue(document, substring);
                return calculate(aggregatedValue, document, subdocumentValue);
            }
        }
        return calculateDefault(((Number) aggregatedValue));
    }

    protected abstract int calculate(Integer aggregatedValue, int value);

    protected abstract long calculate(Long aggregatedValue, long value);

    protected abstract double calculate(Double aggregatedValue, double value);

    protected Number calculateDefault(Number aggregatedValue) {
        return aggregatedValue;
    }

}
