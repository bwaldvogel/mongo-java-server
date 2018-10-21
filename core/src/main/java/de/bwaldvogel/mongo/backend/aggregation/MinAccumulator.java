package de.bwaldvogel.mongo.backend.aggregation;

import de.bwaldvogel.mongo.backend.ValueComparator;

class MinAccumulator extends ComparingAccumulator {

    MinAccumulator(Object expression) {
        super(expression, new ValueComparator());
    }

}
