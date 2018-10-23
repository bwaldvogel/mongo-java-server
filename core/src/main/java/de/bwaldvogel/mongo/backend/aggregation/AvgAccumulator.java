package de.bwaldvogel.mongo.backend.aggregation;

import de.bwaldvogel.mongo.backend.Utils;

class AvgAccumulator extends Accumulator {

    private Number sum = 0;
    private int count;

    AvgAccumulator(Object expression) {
        super(expression);
    }

    @Override
    public void aggregate(Object value) {
        if (value instanceof Number) {
            sum = Utils.addNumbers(sum, (Number) value);
            count++;
        }
    }

    @Override
    public Object getResult() {
        if (count == 0) {
            return null;
        } else {
            return sum.doubleValue() / count;
        }
    }
}
