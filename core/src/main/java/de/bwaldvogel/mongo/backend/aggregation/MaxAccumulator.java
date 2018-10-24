package de.bwaldvogel.mongo.backend.aggregation;

import de.bwaldvogel.mongo.backend.ValueComparator;

class MaxAccumulator extends ComparingAccumulator {

    MaxAccumulator(String field, Object expression) {
        super(field, expression, new ValueComparator().reversed());
    }

}
