package de.bwaldvogel.mongo.backend.aggregation;

import de.bwaldvogel.mongo.backend.Missing;

record TwoParameters(Object first, Object second) {

    boolean isAnyNull() {
        return Missing.isNullOrMissing(first) || Missing.isNullOrMissing(second);
    }
}
