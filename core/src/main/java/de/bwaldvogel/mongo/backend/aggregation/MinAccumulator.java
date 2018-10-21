package de.bwaldvogel.mongo.backend.aggregation;

class MinAccumulator extends AbstractNumberAccumulator {

    MinAccumulator(String field, Object expression) {
        super(field, expression);
    }

    @Override
    protected int calculate(Integer aggregatedValue, int value) {
        if (aggregatedValue == null) {
            return value;
        }
        return Math.min(aggregatedValue, value);
    }

    @Override
    protected long calculate(Long aggregatedValue, long value) {
        if (aggregatedValue == null) {
            return value;
        }
        return Math.min(aggregatedValue, value);
    }

    @Override
    protected double calculate(Double aggregatedValue, double value) {
        if (aggregatedValue == null) {
            return value;
        }
        return Math.min(aggregatedValue, value);
    }

}
