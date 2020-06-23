package de.bwaldvogel.mongo.backend.aggregation.accumulator;

import de.bwaldvogel.mongo.backend.Missing;

public class FirstAccumulator extends Accumulator {

    private Object firstValue;
    private boolean first = true;

    public FirstAccumulator(String field, Object expression) {
        super(field, expression);
    }

    @Override
    public void aggregate(Object value) {
        if (first && !(value instanceof Missing)) {
            firstValue = value;
            first = false;
        }
    }

    @Override
    public Object getResult() {
        return firstValue;
    }
}
