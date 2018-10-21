package de.bwaldvogel.mongo.backend.aggregation;

import de.bwaldvogel.mongo.bson.Document;

class SumAccumulator extends AbstractNumberAccumulator {

    SumAccumulator(String field, Object expression) {
        super(field, expression);
    }

    @Override
    protected void initialize(Document result, String field, Object expression) {
        result.put(field, getInitialValue(expression));
    }

    private Object getInitialValue(Object expression) {
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
    protected int calculate(Integer aggregatedValue, int value) {
        if (aggregatedValue == null) {
            return value;
        }
        return aggregatedValue + value;
    }

    @Override
    protected long calculate(Long aggregatedValue, long value) {
        if (aggregatedValue == null) {
            return value;
        }
        return aggregatedValue + value;
    }

    @Override
    protected double calculate(Double aggregatedValue, double value) {
        if (aggregatedValue == null) {
            return value;
        }
        return aggregatedValue + value;
    }

    @Override
    protected Number calculateDefault(Number aggregatedValue) {
        if (aggregatedValue != null) {
            return aggregatedValue.intValue() + 1;
        } else {
            return null;
        }
    }
}
