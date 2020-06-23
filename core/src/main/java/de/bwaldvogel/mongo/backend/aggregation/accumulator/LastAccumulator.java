package de.bwaldvogel.mongo.backend.aggregation.accumulator;

import de.bwaldvogel.mongo.backend.Missing;

public class LastAccumulator extends Accumulator {

    private Object lastValue;

    public LastAccumulator(String field, Object expression) {
        super(field, expression);
    }

    @Override
    public void aggregate(Object value) {
        if (!(value instanceof Missing)) {
            lastValue = value;
        }
    }

    @Override
    public Object getResult() {
        return lastValue;
    }
}
