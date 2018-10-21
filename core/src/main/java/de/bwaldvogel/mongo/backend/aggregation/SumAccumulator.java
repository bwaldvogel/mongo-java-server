package de.bwaldvogel.mongo.backend.aggregation;

import de.bwaldvogel.mongo.bson.Document;

class SumAccumulator implements Accumulator {

    private final String field;
    private final Number value;

    SumAccumulator(String field, Object value) {
        this.field = field;
        if (value instanceof Number) {
            this.value = (Number) value;
        } else {
            this.value = 1;
        }
    }

    @Override
    public void initialize(Document result) {
        result.put(field, getDefault());
    }

    private Number getDefault() {
        if (value instanceof Integer) {
            return 0;
        } else if (value instanceof Long) {
            return 0L;
        } else if (value instanceof Double) {
            return 0.0;
        } else {
            throw new IllegalArgumentException("Unknown value type: " + value);
        }
    }

    @Override
    public void aggregate(Document result, Document document) {
        Number count = (Number) result.get(field);
        result.put(field, add(count));
    }

    private Number add(Number count) {
        if (value instanceof Integer) {
            return count.intValue() + value.intValue();
        } else if (value instanceof Long) {
            return count.longValue() + value.longValue();
        } else if (value instanceof Double) {
            return count.doubleValue() + value.doubleValue();
        } else {
            throw new IllegalArgumentException("Unknown value type: " + value);
        }
    }
}
