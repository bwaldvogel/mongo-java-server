package de.bwaldvogel.mongo.backend.aggregation;

import de.bwaldvogel.mongo.backend.Utils;

class SumAccumulator extends Accumulator {

    private Number sum = 0;

    SumAccumulator(String field, Object expression) {
        super(field, expression);
    }

    @Override
    public void aggregate(Object value) {
        if (value instanceof Number) {
            sum = Utils.addNumbers(sum, (Number) value);
        }
    }

    @Override
    public Object getResult() {
        return sum;
    }
}
