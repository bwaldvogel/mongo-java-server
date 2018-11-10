package de.bwaldvogel.mongo.backend.aggregation;

import static de.bwaldvogel.mongo.backend.Utils.describeType;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.regex.Pattern;

import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.backend.ValueComparator;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.Missing;
import de.bwaldvogel.mongo.exception.MongoServerError;

public enum Expression {

    $abs {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            return evaluateNumericValue(expressionValue, "$abs", Math::abs);
        }
    },

    $add {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            boolean returnDate = false;
            Number sum = 0;
            for (Object value : expressionValue) {
                Object number = evaluate(value, document);
                if (Utils.isNullOrMissing(number)) {
                    return null;
                }
                if (!(number instanceof Number) && !(number instanceof Date)) {
                    throw new MongoServerError(16554,
                        "$add only supports numeric or date types, not " + describeType(number));
                }
                if (number instanceof Date) {
                    number = ((Date) number).getTime();
                    returnDate = true;
                }
                sum = Utils.addNumbers(sum, (Number) number);
            }
            if (returnDate) {
                return new Date(sum.longValue());
            }
            return sum;
        }
    },

    $and {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            for (Object value : expressionValue) {
                Object evaluatedValue = evaluate(value, document);
                if (!Utils.isTrue(evaluatedValue)) {
                    return false;
                }
            }
            return true;
        }
    },

    $anyElementTrue {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            Object valueInCollection = singleValue(expressionValue, "$anyElementTrue");
            if (!(valueInCollection instanceof Collection)) {
                throw new MongoServerError(17041,
                    "$anyElementTrue's argument must be an array, but is " + describeType(valueInCollection));
            }
            Collection<?> collectionInCollection = (Collection<?>) valueInCollection;
            for (Object value : collectionInCollection) {
                if (Utils.isTrue(value)) {
                    return true;
                }
            }
            return false;
        }
    },

    $allElementsTrue {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            Object valueInCollection = singleValue(expressionValue, "$allElementsTrue");
            if (!(valueInCollection instanceof Collection)) {
                throw new MongoServerError(17040,
                    "$allElementsTrue's argument must be an array, but is " + describeType(valueInCollection));
            }
            Collection<?> collectionInCollection = (Collection<?>) valueInCollection;
            for (Object value : collectionInCollection) {
                if (!Utils.isTrue(value)) {
                    return false;
                }
            }
            return true;
        }
    },

    $arrayElemAt {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            TwoParameters parameters = requireTwoParameters(expressionValue, "$arrayElemAt");

            if (parameters.isAnyNull()) {
                return null;
            }

            Object firstValue = parameters.getFirst();
            Object secondValue = parameters.getSecond();
            if (!(firstValue instanceof List<?>)) {
                throw new MongoServerError(28689,
                    "$arrayElemAt's first argument must be an array, but is " + describeType(firstValue));
            }
            if (!(secondValue instanceof Number)) {
                throw new MongoServerError(28690,
                    "$arrayElemAt's second argument must be a numeric value, but is " + describeType(secondValue));
            }
            List<?> collection = (List<?>) firstValue;
            int index = ((Number) secondValue).intValue();
            if (index < 0) {
                index = collection.size() + index;
            }
            if (index < 0 || index >= collection.size()) {
                return null;
            } else {
                return collection.get(index);
            }
        }
    },

    $ceil {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            return evaluateNumericValue(expressionValue, "$ceil", a -> toIntOrLong(Math.ceil(a)));
        }
    },

    $cmp {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            return evaluateComparison(expressionValue, "$cmp");
        }
    },

    $concat {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            StringBuilder result = new StringBuilder();
            for (Object value : expressionValue) {
                Object evaluatedValue = evaluate(value, document);
                if (Utils.isNullOrMissing(evaluatedValue)) {
                    return null;
                }
                if (!(evaluatedValue instanceof String)) {
                    throw new MongoServerError(16702,
                        "$concat only supports strings, not " + describeType(evaluatedValue));
                }
                result.append(evaluatedValue);
            }
            return result.toString();
        }
    },

    $concatArrays {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            List<Object> result = new ArrayList<>();
            for (Object value : expressionValue) {
                Object evaluatedValue = evaluate(value, document);
                if (Utils.isNullOrMissing(evaluatedValue)) {
                    return null;
                }
                if (!(evaluatedValue instanceof Collection<?>)) {
                    throw new MongoServerError(28664,
                        "$concatArrays only supports arrays, not " + describeType(evaluatedValue));
                }
                result.addAll((Collection<?>) evaluatedValue);
            }
            return result;
        }
    },

    $cond {
        @Override
        Object apply(List<?> expressionValue, Document document) {

            final Object ifExpression;
            final Object thenExpression;
            final Object elseExpression;

            if (expressionValue.size() == 1 && expressionValue.get(0) instanceof Document) {
                Document condDocument = (Document) expressionValue.get(0);
                List<String> requiredKeys = Arrays.asList("if", "then", "else");
                for (String requiredKey : requiredKeys) {
                    if (!condDocument.containsKey(requiredKey)) {
                        throw new MongoServerError(17080, "Missing '" + requiredKey + "' parameter to $cond");
                    }
                }
                for (String key : condDocument.keySet()) {
                    if (!requiredKeys.contains(key)) {
                        throw new MongoServerError(17083, "Unrecognized parameter to $cond: " + key);
                    }
                }

                ifExpression = condDocument.get("if");
                thenExpression = condDocument.get("then");
                elseExpression = condDocument.get("else");
            } else {
                List<?> collection = requireCollectionInSize(expressionValue, "$cond", 3);
                ifExpression = collection.get(0);
                thenExpression = collection.get(1);
                elseExpression = collection.get(2);
            }

            if (Utils.isTrue(evaluate(ifExpression, document))) {
                return evaluate(thenExpression, document);
            } else {
                return evaluate(elseExpression, document);
            }
        }
    },

    $dayOfMonth {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            return evaluateDate(expressionValue, "$dayOfMonth", LocalDate::getDayOfMonth);
        }
    },

    $dayOfWeek {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            return evaluateDate(expressionValue, "$dayOfWeek", date -> date.getDayOfWeek().getValue());
        }
    },

    $dayOfYear {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            return evaluateDate(expressionValue, "$dayOfYear", LocalDate::getDayOfYear);
        }
    },

    $divide {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            TwoParameters parameters = requireTwoParameters(expressionValue, "$divide");
            Object one = parameters.getFirst();
            Object other = parameters.getSecond();

            if (parameters.isAnyNull()) {
                return null;
            }

            if (!(one instanceof Number && other instanceof Number)) {
                throw new MongoServerError(16609,
                    "$divide only supports numeric types, not " + describeType(one) + " and " + describeType(other));
            }

            double a = ((Number) one).doubleValue();
            double b = ((Number) other).doubleValue();
            if (Double.compare(b, 0.0) == 0) {
                throw new MongoServerError(16608, "can't $divide by zero");
            }
            return a / b;
        }
    },

    $eq {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            return evaluateComparison(expressionValue, "$eq", v -> v == 0);
        }
    },

    $exp {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            return evaluateNumericValue(expressionValue, "$exp", Math::exp);
        }
    },

    $filter {
        @Override
        Object apply(Object expressionValue, Document document) {
            if (!(expressionValue instanceof Document)) {
                throw new MongoServerError(28646, "$filter only supports an object as its argument");
            }
            Document filterExpression = (Document) expressionValue;

            List<String> requiredKeys = Arrays.asList("input", "cond");
            for (String requiredKey : requiredKeys) {
                if (!filterExpression.containsKey(requiredKey)) {
                    throw new MongoServerError(28648, "Missing '" + requiredKey + "' parameter to $filter");
                }
            }

            for (String key : filterExpression.keySet()) {
                if (!Arrays.asList("input", "cond", "as").contains(key)) {
                    throw new MongoServerError(28647, "Unrecognized parameter to $filter: " + key);
                }
            }

            Object input = evaluate(filterExpression.get("input"), document);
            Object as = evaluate(filterExpression.getOrDefault("as", "this"), document);
            if (!(as instanceof String)) {
                throw new MongoServerError(16866, "empty variable names are not allowed");
            }
            if (input == null) {
                return null;
            }

            if (!(input instanceof Collection)) {
                throw new MongoServerError(28651, "input to $filter must be an array not string");
            }

            Collection<?> inputCollection = (Collection<?>) input;

            String key = "$" + as;
            Document documentForCondition = document.clone();
            if (documentForCondition.containsKey(key)) {
                throw new IllegalArgumentException("Document contains " + key + ". This must not happen");
            }
            List<Object> result = new ArrayList<>();
            for (Object inputValue : inputCollection) {
                Object evaluatedInputValue = evaluate(inputValue, document);
                documentForCondition.put(key, evaluatedInputValue);
                if (Utils.isTrue(evaluate(filterExpression.get("cond"), documentForCondition))) {
                    result.add(evaluatedInputValue);
                }
            }
            return result;
        }

        @Override
        Object apply(List<?> expressionValue, Document document) {
            throw new UnsupportedOperationException("must not be invoked");
        }
    },

    $floor {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            return evaluateNumericValue(expressionValue, "$floor", a -> toIntOrLong(Math.floor(a)));
        }
    },

    $gt {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            return evaluateComparison(expressionValue, "$gt", v -> v > 0);
        }
    },

    $gte {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            return evaluateComparison(expressionValue, "$gte", v -> v >= 0);
        }
    },

    $hour {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            return evaluateTime(expressionValue, "$hour", LocalTime::getHour);
        }
    },

    $ifNull {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            TwoParameters parameters = requireTwoParameters(expressionValue, "$ifNull");
            Object expression = parameters.getFirst();
            if (!Utils.isNullOrMissing(expression)) {
                return expression;
            } else {
                return parameters.getSecond();
            }
        }
    },

    $in {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            TwoParameters parameters = requireTwoParameters(expressionValue, "$in");
            Object needle = parameters.getFirst();
            Object haystack = parameters.getSecond();

            if (!(haystack instanceof Collection)) {
                throw new MongoServerError(40081, "$in requires an array as a second argument, found: " + describeType(haystack));
            }

            return ((Collection<?>) haystack).contains(needle);
        }
    },

    $indexOfArray {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            if (expressionValue.size() < 2 || expressionValue.size() > 4) {
                throw new MongoServerError(28667,
                    "Expression $indexOfArray takes at least 2 arguments, and at most 4, but " + expressionValue.size() + " were passed in.");
            }

            Object first = expressionValue.get(0);
            if (Utils.isNullOrMissing(first)) {
                return null;
            }
            if (!(first instanceof List<?>)) {
                throw new MongoServerError(40090,
                    "$indexOfArray requires an array as a first argument, found: " + describeType(first));
            }
            List<?> elementsToSearchIn = (List<?>) first;

            int start = 0;
            if (expressionValue.size() >= 3) {
                Object startValue = expressionValue.get(2);
                start = requireIntegral(startValue, "$indexOfArray", "starting index");
                start = Math.min(start, elementsToSearchIn.size());
            }

            int end = elementsToSearchIn.size();
            if (expressionValue.size() >= 4) {
                Object endValue = expressionValue.get(3);
                end = requireIntegral(endValue, "$indexOfArray", "ending index");
                end = Math.min(Math.max(start, end), elementsToSearchIn.size());
            }

            elementsToSearchIn = elementsToSearchIn.subList(start, end);
            int index = elementsToSearchIn.indexOf(expressionValue.get(1));
            if (index >= 0) {
                return index + start;
            }
            return index;
        }
    },

    $indexOfBytes {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            return evaluateIndexOf(expressionValue, "$indexOfBytes", this::toList,
                40091, 40092);
        }

        private List<Byte> toList(String input) {
            List<Byte> bytes = new ArrayList<>();
            for (byte value : input.getBytes(StandardCharsets.UTF_8)) {
                bytes.add(value);
            }
            return bytes;
        }

    },

    $indexOfCP {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            return evaluateIndexOf(expressionValue, "$indexOfCP", this::toList,
                40093, 40094);
        }

        private List<Character> toList(String input) {
            List<Character> characters = new ArrayList<>();
            for (char value : input.toCharArray()) {
                characters.add(value);
            }
            return characters;
        }

    },

    $isArray {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            Object value = singleValue(expressionValue, "$isArray");
            return (value instanceof List);
        }
    },

    $literal {
        @Override
        Object apply(Object expressionValue, Document document) {
            return expressionValue;
        }

        @Override
        Object apply(List<?> expressionValue, Document document) {
            throw new UnsupportedOperationException("must not be invoked");
        }
    },

    $ln {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            return evaluateNumericValue(expressionValue, "$ln", Math::log);
        }
    },

    $log {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            return evaluateNumericValue(expressionValue, "$log", Math::log);
        }
    },

    $log10 {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            return evaluateNumericValue(expressionValue, "$log10", Math::log10);
        }
    },

    $lt {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            return evaluateComparison(expressionValue, "$lt", v -> v < 0);
        }
    },

    $lte {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            return evaluateComparison(expressionValue, "$lte", v -> v <= 0);
        }
    },

    $minute {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            return evaluateTime(expressionValue, "$minute", LocalTime::getMinute);
        }
    },

    $mod {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            TwoParameters parameters = requireTwoParameters(expressionValue, "$mod");
            Object one = parameters.getFirst();
            Object other = parameters.getSecond();

            if (!(one instanceof Number && other instanceof Number)) {
                throw new MongoServerError(16611,
                    "$mod only supports numeric types, not " + describeType(one) + " and " + describeType(other));
            }

            double a = ((Number) one).doubleValue();
            double b = ((Number) other).doubleValue();
            return a % b;
        }
    },

    $month {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            return evaluateDate(expressionValue, "$month", date -> date.getMonth().getValue());
        }
    },

    $multiply {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            TwoParameters parameters = requireTwoParameters(expressionValue, "$multiply");
            Object one = parameters.getFirst();
            Object other = parameters.getSecond();

            if (parameters.isAnyNull()) {
                return null;
            }

            if (!(one instanceof Number && other instanceof Number)) {
                throw new MongoServerError(16555,
                    "$multiply only supports numeric types, not " + describeType(one) + " and " + describeType(other));
            }

            double a = ((Number) one).doubleValue();
            double b = ((Number) other).doubleValue();
            return a * b;
        }
    },


    $ne {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            return evaluateComparison(expressionValue, "$ne", v -> v != 0);
        }
    },

    $not {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            Object value = singleValue(expressionValue, "$not");
            return !Utils.isTrue(evaluate(value, document));
        }
    },

    $or {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            for (Object value : expressionValue) {
                Object evaluatedValue = evaluate(value, document);
                if (Utils.isTrue(evaluatedValue)) {
                    return true;
                }
            }
            return false;
        }
    },


    $pow {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            TwoParameters parameters = requireTwoParameters(expressionValue, "$pow");
            if (parameters.isAnyNull()) {
                return null;
            }

            Object base = parameters.getFirst();
            Object exponent = parameters.getSecond();

            if (!(base instanceof Number)) {
                throw new MongoServerError(28762,
                    "$pow's base must be numeric, not " + describeType(base));
            }

            if (!(exponent instanceof Number)) {
                throw new MongoServerError(28763,
                    "$pow's exponent must be numeric, not " + describeType(exponent));
            }

            double a = ((Number) base).doubleValue();
            double b = ((Number) exponent).doubleValue();
            return Math.pow(a, b);
        }
    },

    $range {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            if (expressionValue.size() < 2 || expressionValue.size() > 3) {
                throw new MongoServerError(28667, "Expression $range takes at least 2 arguments, and at most 3, but " + expressionValue.size() + " were passed in.");
            }

            Object first = expressionValue.get(0);
            Object second = expressionValue.get(1);

            int start = toInt(first, 34443, 34444, "starting value");
            int end = toInt(second, 34445, 34446, "ending value");

            final int step;
            if (expressionValue.size() > 2) {
                Object third = expressionValue.get(2);
                step = toInt(third, 34447, 34448, "step value");
                if (step == 0) {
                    throw new MongoServerError(34449, "$range requires a non-zero step value");
                }
            } else {
                step = 1;
            }

            List<Integer> values = new ArrayList<>();
            if (step > 0) {
                for (int i = start; i < end; i += step) {
                    values.add(i);
                }
            } else {
                for (int i = start; i > end; i += step) {
                    values.add(i);
                }
            }
            return values;
        }

        private int toInt(Object object, int errorCodeIfNotANumber, int errorCodeIfNonInt, String errorMessage) {
            if (!(object instanceof Number)) {
                throw new MongoServerError(errorCodeIfNotANumber, "$range requires a numeric " + errorMessage + ", found value of type: " + describeType(object));
            }
            Number number = (Number) object;
            int value = number.intValue();
            if (number.doubleValue() != value) {
                throw new MongoServerError(errorCodeIfNonInt,
                    "$range requires a " + errorMessage + " that can be represented as a 32-bit integer, found value: " + number);
            }
            return value;
        }
    },

    $second {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            return evaluateTime(expressionValue, "$second", LocalTime::getSecond);
        }
    },

    $setDifference {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            TwoParameters parameters = requireTwoParameters(expressionValue, "$setDifference");

            if (parameters.isAnyNull()) {
                return null;
            }

            Object first = parameters.getFirst();
            Object second = parameters.getSecond();

            if (!(first instanceof Collection)) {
                throw new MongoServerError(17048,
                    "both operands of $setDifference must be arrays. First argument is of type: " + describeType(first));
            }

            if (!(second instanceof Collection)) {
                throw new MongoServerError(17049,
                    "both operands of $setDifference must be arrays. First argument is of type: " + describeType(second));
            }

            Set<Object> result = new LinkedHashSet<>((Collection<?>) first);
            result.removeAll((Collection<?>) second);
            return result;
        }

    },

    $setEquals {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            if (expressionValue.size() < 2) {
                throw new MongoServerError(17045, "$setEquals needs at least two arguments had: " + expressionValue.size());
            }

            final Set<Set<?>> result = new HashSet<>();
            for (Object value : expressionValue) {
                Object evaluatedValue = evaluate(value, document);
                if (!(evaluatedValue instanceof Collection)) {
                    throw new MongoServerError(17044, "All operands of $setEquals must be arrays. One argument is of type: " + describeType(evaluatedValue));
                }
                result.add(new HashSet<>((Collection<?>) evaluatedValue));
            }
            return result.size() == 1;
        }
    },

    $setIntersection {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            Set<?> result = null;
            for (Object value : expressionValue) {
                Object evaluatedValue = evaluate(value, document);
                if (Utils.isNullOrMissing(evaluatedValue)) {
                    return null;
                }
                if (!(evaluatedValue instanceof Collection)) {
                    throw new MongoServerError(17047, "All operands of $setIntersection must be arrays. One argument is of type: " + describeType(evaluatedValue));
                }
                if (result == null) {
                    result = new LinkedHashSet<>((Collection<?>) evaluatedValue);
                } else {
                    result.retainAll((Collection<?>) evaluatedValue);
                }
            }
            if (result == null) {
                return Collections.emptySet();
            }
            return result;
        }
    },

    $setIsSubset {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            TwoParameters parameters = requireTwoParameters(expressionValue, "$setIsSubset");
            Object first = parameters.getFirst();
            Object second = parameters.getSecond();

            if (!(first instanceof Collection<?>)) {
                throw new MongoServerError(17046, "both operands of $setIsSubset must be arrays. First argument is of type: " + describeType(first));
            }

            if (!(second instanceof Collection<?>)) {
                throw new MongoServerError(17042, "both operands of $setIsSubset must be arrays. Second argument is of type: " + describeType(second));
            }

            Set<?> one = new HashSet<>((Collection<?>) first);
            Set<?> other = new HashSet<>((Collection<?>) second);
            return other.containsAll(one);
        }
    },

    $setUnion {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            Set<Object> result = new LinkedHashSet<>();
            for (Object value : expressionValue) {
                Object evaluatedValue = evaluate(value, document);
                if (Utils.isNullOrMissing(evaluatedValue)) {
                    return null;
                }
                if (!(evaluatedValue instanceof Collection<?>)) {
                    throw new MongoServerError(17043,
                        "All operands of $setUnion must be arrays. One argument is of type: " + describeType(evaluatedValue));
                }
                result.addAll((Collection<?>) evaluatedValue);
            }
            return result;
        }
    },

    $size {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            Object value = singleValue(expressionValue, "$size");
            if (!(value instanceof Collection)) {
                throw new MongoServerError(17124,
                    "The argument to $size must be an array, but was of type: " + describeType(value));
            }
            Collection<?> collection = (Collection<?>) value;
            return collection.size();
        }
    },

    $slice {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            if (expressionValue.size() < 2 || expressionValue.size() > 3) {
                throw new MongoServerError(28667, "Expression $slice takes at least 2 arguments, and at most 3, but " + expressionValue.size() + " were passed in.");
            }

            Object first = expressionValue.get(0);
            if (Utils.isNullOrMissing(first)) {
                return null;
            }
            if (!(first instanceof List)) {
                throw new MongoServerError(28724, "First argument to $slice must be an array, but is of type: " + describeType(first));
            }
            List<?> list = (List<?>) first;

            Object second = expressionValue.get(1);
            if (!(second instanceof Number)) {
                throw new MongoServerError(28725, "Second argument to $slice must be a numeric value, but is of type: " + describeType(second));
            }

            final List<?> result;
            if (expressionValue.size() > 2) {
                Object third = expressionValue.get(2);
                if (!(third instanceof Number)) {
                    throw new MongoServerError(28725, "Third argument to $slice must be numeric, but is of type: " + describeType(third));
                }

                Number number = (Number) third;
                if (number.intValue() < 0) {
                    throw new MongoServerError(28729, "Third argument to $slice must be positive: " + third);
                }

                int position = ((Number) second).intValue();
                final int offset;
                if (position >= 0) {
                    offset = Math.min(position, list.size());
                } else {
                    offset = Math.max(0, list.size() + position);
                }

                result = list.subList(offset, Math.min(offset + number.intValue(), list.size()));
            } else {
                int n = ((Number) second).intValue();
                if (n >= 0) {
                    result = list.subList(0, Math.min(n, list.size()));
                } else {
                    result = list.subList(Math.max(0, list.size() + n), list.size());
                }
            }

            return result;
        }
    },

    $split {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            TwoParameters parameters = requireTwoParameters(expressionValue, "$split");
            Object string = parameters.getFirst();
            Object delimiter = parameters.getSecond();

            if (Utils.isNullOrMissing(string)) {
                return null;
            }

            if (!(string instanceof String)) {
                throw new MongoServerError(40085,
                    "$split requires an expression that evaluates to a string as a first argument, found: " + describeType(string));
            }
            if (!(delimiter instanceof String)) {
                throw new MongoServerError(40086,
                    "$split requires an expression that evaluates to a string as a second argument, found: " + describeType(delimiter));
            }

            return ((String) string).split(Pattern.quote((String) delimiter));
        }
    },

    $subtract {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            TwoParameters parameters = requireTwoParameters(expressionValue, "$subtract");
            Object one = parameters.getFirst();
            Object other = parameters.getSecond();

            if (Utils.isNullOrMissing(one) || Utils.isNullOrMissing(other)) {
                return null;
            }

            if (!(one instanceof Number && other instanceof Number)) {
                throw new MongoServerError(16556,
                    "cant $subtract a " + describeType(one) + " from a " + describeType(other));
            }

            return Utils.subtractNumbers((Number) one, (Number) other);
        }
    },

    $sum {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            if (expressionValue.size() == 1) {
                Object singleValue = evaluate(expressionValue.get(0), document);
                if (singleValue instanceof Collection<?>) {
                    return apply(singleValue, document);
                }
            }
            Number sum = 0;
            for (Object value : expressionValue) {
                Object evaluatedValue = evaluate(value, document);
                if (evaluatedValue instanceof Number) {
                    sum = Utils.addNumbers(sum, (Number) evaluatedValue);
                }
            }
            return sum;
        }
    },

    $sqrt {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            return evaluateNumericValue(expressionValue, "$sqrt", Math::sqrt);
        }
    },

    $trunc {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            return evaluateNumericValue(expressionValue, "$trunc", a -> toIntOrLong(a.longValue()));
        }
    },

    $year {
        @Override
        Object apply(List<?> expressionValue, Document document) {
            return evaluateDate(expressionValue, "$year", LocalDate::getYear);
        }
    },

    ;

    Object apply(Object expressionValue, Document document) {
        List<Object> evaluatedValues = new ArrayList<>();
        if (!(expressionValue instanceof Collection)) {
            evaluatedValues.add(evaluate(expressionValue, document));
        } else {
            for (Object value : ((Collection<?>) expressionValue)) {
                evaluatedValues.add(evaluate(value, document));
            }
        }
        return apply(evaluatedValues, document);
    }

    abstract Object apply(List<?> expressionValue, Document document);

    public static Object evaluate(Object expression, Document document) {
        if (expression instanceof String && ((String) expression).startsWith("$")) {
            String value = ((String) expression).substring(1);
            if (value.startsWith("$")) {
                if (value.equals("$ROOT")) {
                    return document;
                } else if (value.startsWith("$ROOT.")) {
                    String subKey = value.substring("$ROOT.".length());
                    return Utils.getSubdocumentValue(document, subKey);
                }
                Object subdocumentValue = Utils.getSubdocumentValue(document, value);
                if (!(subdocumentValue instanceof Missing)) {
                    return subdocumentValue;
                }
                String variable = value.substring(1);
                throw new MongoServerError(17276, "Use of undefined variable: " + variable);
            }
            return Utils.getSubdocumentValue(document, value);
        } else if (expression instanceof Document) {
            return evaluateDocumentExpression((Document) expression, document);
        } else {
            return expression;
        }
    }

    private static Object evaluateDocumentExpression(Document expression, Document document) {
        Document result = new Document();
        for (Entry<String, Object> entry : expression.entrySet()) {
            String expressionKey = entry.getKey();
            Object expressionValue = entry.getValue();
            if (expressionKey.startsWith("$")) {
                if (expression.keySet().size() > 1) {
                    throw new MongoServerError(15983, "An object representing an expression must have exactly one field: " + expression);
                }

                final Expression exp;
                try {
                    exp = valueOf(expressionKey);
                } catch (IllegalArgumentException ex) {
                    throw new MongoServerError(168, "InvalidPipelineOperator", "Unrecognized expression '" + expressionKey + "'");
                }
                return exp.apply(expressionValue, document);
            } else {
                result.put(expressionKey, evaluate(expressionValue, document));
            }
        }
        return result;
    }

    private static ZonedDateTime getZonedDateTime(Object value, String expressionName) {
        ZoneId timezone = ZoneId.systemDefault();
        if (value instanceof Document) {
            Document document = (Document) value;
            if (!document.containsKey("date")) {
                throw new MongoServerError(40539, "missing 'date' argument to " + expressionName + ", provided: " + value);
            }
            value = document.get("date");

            Object timezoneExpression = document.get("timezone");
            if (timezoneExpression != null) {
                timezone = ZoneId.of(timezoneExpression.toString());
            }
        }
        if (!(value instanceof Date)) {
            throw new MongoServerError(16006, "can't convert from " + describeType(value) + " to Date");
        }

        Instant instant = ((Date) value).toInstant();
        return ZonedDateTime.ofInstant(instant, timezone);
    }

    private static int evaluateComparison(List<?> expressionValue, String expressionName) {
        TwoParameters parameters = requireTwoParameters(expressionValue, expressionName);
        return new ValueComparator().compare(parameters.getFirst(), parameters.getSecond());
    }

    private static boolean evaluateComparison(List<?> expressionValue, String expressionName, IntPredicate comparison) {
        int comparisonResult = evaluateComparison(expressionValue, expressionName);
        return comparison.test(comparisonResult);
    }

    private static <T> T evaluateDateTime(List<?> expressionValue, String expressionName, Function<ZonedDateTime, T> dateFunction) {
        Object value = singleValue(expressionValue, expressionName);
        if (Utils.isNullOrMissing(value)) {
            return null;
        }

        ZonedDateTime zonedDateTime = getZonedDateTime(value, expressionName);
        return dateFunction.apply(zonedDateTime);
    }

    private static <T> T evaluateDate(List<?> expressionValue, String expressionName, Function<LocalDate, T> dateFunction) {
        return evaluateDateTime(expressionValue, expressionName, zonedDateTime -> dateFunction.apply(zonedDateTime.toLocalDate()));
    }

    private static <T> T evaluateTime(List<?> expressionValue, String expressionName, Function<LocalTime, T> timeFunction) {
        return evaluateDateTime(expressionValue, expressionName, zonedDateTime -> timeFunction.apply(zonedDateTime.toLocalTime()));
    }

    private static Object singleValue(List<?> list, String expressionName) {
        if (list.size() != 1) {
            throw new MongoServerError(16020, "Expression " + expressionName + " takes exactly 1 arguments. " + list.size() + " were passed in.");
        }
        return list.get(0);
    }

    private static List<?> requireCollectionInSize(List<?> value, String expressionName, int expectedCollectionSize) {
        if (value.size() != expectedCollectionSize) {
            throw new MongoServerError(16020, "Expression " + expressionName + " takes exactly " + expectedCollectionSize + " arguments. " + value.size() + " were passed in.");
        }
        return value;
    }

    private static TwoParameters requireTwoParameters(List<?> value, String expressionName) {
        List<?> parameters = requireCollectionInSize(value, expressionName, 2);
        return new TwoParameters(parameters.get(0), parameters.get(1));
    }

    private static class TwoParameters {
        private final Object first;
        private final Object second;

        private TwoParameters(Object first, Object second) {
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

    private static Number evaluateNumericValue(List<?> expressionValue, String expressionName, Function<Double, ? extends Number> function) {
        Object value = singleValue(expressionValue, expressionName);
        if (Utils.isNullOrMissing(value)) {
            return null;
        }
        if (!(value instanceof Number)) {
            throw new MongoServerError(28765,
                expressionName + " only supports numeric types, not " + describeType(value));
        }
        Number number = (Number) value;
        if (Double.isNaN(number.doubleValue())) {
            return number;
        }
        return function.apply(number.doubleValue());
    }

    private static Number toIntOrLong(double value) {
        long number = (long) value;
        if (number < Integer.MIN_VALUE || number > Integer.MAX_VALUE) {
            return number;
        } else {
            return Math.toIntExact(number);
        }
    }

    private static int requireIntegral(Object value, String expressionName, String name) {
        if (!(value instanceof Number)) {
            throw new MongoServerError(40096,
                expressionName + " requires an integral " + name + ", found a value of type: " + describeType(value) + ", with value: \"" + value + "\"");
        }
        Number number = (Number) value;
        int intValue = number.intValue();
        if (intValue < 0) {
            throw new MongoServerError(40097, expressionName + " requires a nonnegative " + name + ", found: " + intValue);
        }
        return intValue;
    }

    private static <T> Object evaluateIndexOf(List<?> expressionValue, String expressionName,
                                              Function<String, List<T>> toList,
                                              int errorCodeFirstParameterTypeMismatch,
                                              int errorCodeSecondParameterTypeMismatch) {
        if (expressionValue.size() < 2 || expressionValue.size() > 4) {
            throw new MongoServerError(28667,
                "Expression " + expressionName + " takes at least 2 arguments, and at most 4, but " + expressionValue.size() + " were passed in.");
        }

        Object first = expressionValue.get(0);
        if (Utils.isNullOrMissing(first)) {
            return null;
        }
        if (!(first instanceof String)) {
            throw new MongoServerError(errorCodeFirstParameterTypeMismatch,
                expressionName + " requires a string as the first argument, found: " + describeType(first));
        }
        List<T> elementsToSearchIn = toList.apply((String) first);

        Object searchValue = expressionValue.get(1);
        if (!(searchValue instanceof String)) {
            throw new MongoServerError(errorCodeSecondParameterTypeMismatch,
                expressionName + " requires a string as the second argument, found: " + describeType(searchValue));
        }
        List<T> search = toList.apply((String) searchValue);

        int start = 0;
        if (expressionValue.size() >= 3) {
            Object startValue = expressionValue.get(2);
            start = requireIntegral(startValue, expressionName, "starting index");
            start = Math.min(start, elementsToSearchIn.size());
        }

        int end = elementsToSearchIn.size();
        if (expressionValue.size() >= 4) {
            Object endValue = expressionValue.get(3);
            end = requireIntegral(endValue, expressionName, "ending index");
            end = Math.min(Math.max(start, end), elementsToSearchIn.size());
        }

        elementsToSearchIn = elementsToSearchIn.subList(start, end);
        int index = Collections.indexOfSubList(elementsToSearchIn, search);
        if (index >= 0) {
            return index + start;
        }
        return index;
    }

}
