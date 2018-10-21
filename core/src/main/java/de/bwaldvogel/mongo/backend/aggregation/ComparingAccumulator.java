package de.bwaldvogel.mongo.backend.aggregation;

import java.util.Comparator;

class ComparingAccumulator extends Accumulator {

    private final Comparator<Object> comparator;
    private Object result;

    ComparingAccumulator(Object expression, Comparator<Object> comparator) {
        super(expression);
        this.comparator = comparator;
    }

    @Override
    public void aggregate(Object value) {
        if (value == null) {
            return;
        }
        if (result == null) {
            result = value;
        } else {
            if (comparator.compare(value, result) < 0) {
                result = value;
            }
        }
    }

    @Override
    public Object getResult() {
        return result;
    }
}
