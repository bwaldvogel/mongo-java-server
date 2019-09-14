package de.bwaldvogel.mongo.backend.aggregation;

class TwoNumericParameters {

    private final Number first;
    private final Number second;

    TwoNumericParameters(Number first, Number second) {
        this.first = first;
        this.second = second;
    }

    public Number getFirst() {
        return first;
    }

    public Number getSecond() {
        return second;
    }

    double getFirstAsDouble() {
        return first.doubleValue();
    }

    double getSecondAsDouble() {
        return second.doubleValue();
    }

}
