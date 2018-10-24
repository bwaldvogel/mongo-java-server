package de.bwaldvogel.mongo.backend.aggregation;

import de.bwaldvogel.mongo.backend.ValueComparator;

class MinAccumulator extends ComparingAccumulator {

    MinAccumulator(String field, Object expression) {
        super(field, expression, new ValueComparator());
    }

}
