package de.bwaldvogel.mongo.backend.aggregation;

record TwoNumericParameters(Number first, Number second) {

    double firstAsDouble() {
        return first.doubleValue();
    }

    double secondAsDouble() {
        return second.doubleValue();
    }
}
