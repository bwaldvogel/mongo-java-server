package de.bwaldvogel.mongo.backend.aggregation.accumulator;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Set;

public class AddToSetAccumulator extends Accumulator {

    private Set<Object> result = new LinkedHashSet<>();

    public AddToSetAccumulator(String field, Object expression) {
        super(field, expression);
    }

    @Override
    public void aggregate(Object value) {
        if (value == null) {
            return;
        }
        result.add(value);
    }

    @Override
    public Object getResult() {
        return new ArrayList<>(result);
    }
}
