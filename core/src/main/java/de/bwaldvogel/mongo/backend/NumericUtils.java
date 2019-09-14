package de.bwaldvogel.mongo.backend;

public class NumericUtils {

    @FunctionalInterface
    public interface DoubleCalculation {
        double apply(double a, double b);
    }

    @FunctionalInterface
    public interface LongCalculation {
        long apply(long a, long b);
    }

    private static Number calculate(Number a, Number b,
                                    LongCalculation longCalculation, DoubleCalculation doubleCalculation) {
        if (a instanceof Double || b instanceof Double) {
            return Double.valueOf(doubleCalculation.apply(a.doubleValue(), b.doubleValue()));
        } else if (a instanceof Float || b instanceof Float) {
            double result = doubleCalculation.apply(a.doubleValue(), b.doubleValue());
            return Float.valueOf((float) result);
        } else if (a instanceof Long || b instanceof Long) {
            return Long.valueOf(longCalculation.apply(a.longValue(), b.longValue()));
        } else if (a instanceof Integer || b instanceof Integer) {
            long result = longCalculation.apply(a.longValue(), b.longValue());
            int intResult = (int) result;
            if (intResult == result) {
                return Integer.valueOf(intResult);
            } else {
                return Long.valueOf(result);
            }
        } else if (a instanceof Short || b instanceof Short) {
            long result = longCalculation.apply(a.longValue(), b.longValue());
            short shortResult = (short) result;
            if (shortResult == result) {
                return Short.valueOf(shortResult);
            } else {
                return Long.valueOf(result);
            }
        } else {
            throw new UnsupportedOperationException("cannot calculate on " + a + " and " + b);
        }
    }

    public static Number addNumbers(Number one, Number other) {
        return calculate(one, other, Long::sum, Double::sum);
    }

    public static Number subtractNumbers(Number one, Number other) {
        return calculate(one, other, (a, b) -> a - b, (a, b) -> a - b);
    }

    public static Number multiplyNumbers(Number one, Number other) {
        return calculate(one, other, (a, b) -> a * b, (a, b) -> a * b);
    }

}
