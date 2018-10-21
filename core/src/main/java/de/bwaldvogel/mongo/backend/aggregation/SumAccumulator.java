package de.bwaldvogel.mongo.backend.aggregation;

class SumAccumulator extends Accumulator {

    private Number sum;

    SumAccumulator(Object expression) {
        super(expression);
    }

    @Override
    public void aggregate(Object value) {
        if (value instanceof Number) {
            Number numberValue = (Number) value;
            if (sum == null) {
                sum = numberValue;
            } else {
                if (sum instanceof Double || value instanceof Double) {
                    sum = sum.doubleValue() + ((Number) value).doubleValue();
                } else if (sum instanceof Integer && value instanceof Integer) {
                    sum = sum.intValue() + ((Integer) value).intValue();
                } else if (value instanceof Long) {
                    sum = sum.longValue() + ((Long) value).longValue();
                } else {
                    throw new UnsupportedOperationException();
                }
            }
        }
    }

    @Override
    public Object getResult() {
        if (sum != null) {
            return sum;
        } else {
            return 0;
        }
    }
}
