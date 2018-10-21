package de.bwaldvogel.mongo.backend.aggregation;

import de.bwaldvogel.mongo.backend.ValueComparator;

class MaxAccumulator extends ComparingAccumulator {

    MaxAccumulator(Object expression) {
        super(expression, new ValueComparator().reversed());
    }

}
