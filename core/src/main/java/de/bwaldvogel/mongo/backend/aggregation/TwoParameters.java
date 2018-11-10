package de.bwaldvogel.mongo.backend.aggregation;

import de.bwaldvogel.mongo.backend.Utils;

class TwoParameters {
    private final Object first;
    private final Object second;

    TwoParameters(Object first, Object second) {
        this.first = first;
        this.second = second;
    }

    boolean isAnyNull() {
        return Utils.isNullOrMissing(first) || Utils.isNullOrMissing(second);
    }

    Object getFirst() {
        return first;
    }

    Object getSecond() {
        return second;
    }
}
