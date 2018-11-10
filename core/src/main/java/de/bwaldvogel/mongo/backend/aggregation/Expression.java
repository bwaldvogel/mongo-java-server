package de.bwaldvogel.mongo.backend.aggregation;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.IntPredicate;

import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.backend.ValueComparator;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;

public enum Expression {

    $abs {
        @Override
        Object apply(Object expressionValue, Document document) {
            Object value = evaluate(expressionValue, document);
            if (value == null) {
                return null;
            } else if (value instanceof Double) {
                return Math.abs(((Double) value).doubleValue());
            } else if (value instanceof Long) {
                return Math.abs(((Long) value).longValue());
            } else if (value instanceof Integer) {
                return Math.abs(((Integer) value).intValue());
            } else {
                throw new MongoServerError(28765, "$abs only supports numeric types, not " + value.getClass());
            }
        }
    },

    $add {
        @Override
        Object apply(Object expressionValue, Document document) {
            Object value = evaluate(expressionValue, document);
            if (!(value instanceof Collection)) {
                throw new MongoServerError(16020, "Expression $add takes exactly 2 arguments. 1 were passed in.");
            }
            Collection<?> values = (Collection<?>) value;
            if (values.size() != 2) {
                throw new MongoServerError(16020, "Expression $add takes exactly 2 arguments. " + values.size() + " were passed in.");
            }

            Iterator<?> iterator = values.iterator();
            Object one = evaluate(iterator.next(), document);
            Object other = evaluate(iterator.next(), document);

            if (!(one instanceof Number && other instanceof Number)) {
                throw new MongoServerError(16556,
                    "cant $add a " + describeType(one) + " and a " + describeType(other));
            }

            return Utils.addNumbers((Number) one, (Number) other);
        }
    },

    $and {
        @Override
        Object apply(Object expressionValue, Document document) {
            Object value = evaluate(expressionValue, document);
            if (value == null) {
                return false;
            }
            if (value instanceof Boolean) {
                return ((Boolean) value).booleanValue();
            }
            if (value instanceof Collection<?>) {
                Collection<?> collection = (Collection<?>) value;
                for (Object v : collection) {
                    Object evaluatedValue = evaluate(v, document);
                    if (!Utils.isTrue(evaluatedValue)) {
                        return false;
                    }
                }
            }
            return true;
        }
    },

    $anyElementTrue {
        @Override
        Object apply(Object expressionValue, Document document) {
            Object value = evaluate(expressionValue, document);
            if (!(value instanceof Collection)) {
                throw new MongoServerError(17041, "$anyElementTrue's argument must be an array, but is " + describeType(value));
            }
            Collection<?> collection = (Collection<?>) value;
            if (collection.size() != 1) {
                throw new MongoServerError(16020, "Expression $anyElementTrue takes exactly 1 arguments. " + collection.size() + " were passed in.");
            }
            Object valueInCollection = evaluate(collection.iterator().next(), document);
            if (valueInCollection == null) {
                throw new MongoServerError(17041, "$anyElementTrue's argument must be an array, but is null");
            }
            if (!(valueInCollection instanceof Collection)) {
                throw new MongoServerError(17041, "$anyElementTrue's argument must be an array, but is " + describeType(value));
            }
            Collection<?> collectionInCollection = (Collection<?>) valueInCollection;
            for (Object v : collectionInCollection) {
                if (Utils.isTrue(v)) {
                    return true;
                }
            }
            return false;
        }
    },

    $allElementsTrue {
        @Override
        Object apply(Object expressionValue, Document document) {
            Object value = evaluate(expressionValue, document);
            if (!(value instanceof Collection)) {
                throw new MongoServerError(17040, "$allElementsTrue's argument must be an array, but is " + describeType(value));
            }
            Collection<?> collection = (Collection<?>) value;
            if (collection.size() != 1) {
                throw new MongoServerError(16020, "Expression $allElementsTrue takes exactly 1 arguments. " + collection.size() + " were passed in.");
            }
            Object valueInCollection = evaluate(collection.iterator().next(), document);
            if (valueInCollection == null) {
                throw new MongoServerError(17040, "$allElementsTrue's argument must be an array, but is null");
            }
            if (!(valueInCollection instanceof Collection)) {
                throw new MongoServerError(17040, "$allElementsTrue's argument must be an array, but is " + describeType(value));
            }
            Collection<?> collectionInCollection = (Collection<?>) valueInCollection;
            for (Object v : collectionInCollection) {
                if (!Utils.isTrue(v)) {
                    return false;
                }
            }
            return true;
        }
    },

    $arrayElemAt {
        @Override
        Object apply(Object expressionValue, Document document) {
            Object value = evaluate(expressionValue, document);
            if (!(value instanceof Collection)) {
                throw new MongoServerError(16020, "Expression $arrayElemAt takes exactly 2 arguments. 1 were passed in.");
            }
            Collection<?> parameters = (Collection<?>) value;
            if (parameters.size() != 2) {
                throw new MongoServerError(16020, "Expression $arrayElemAt takes exactly 2 arguments. " + parameters.size() + " were passed in.");
            }
            Iterator<?> iterator = parameters.iterator();
            Object firstValue = evaluate(iterator.next(), document);
            Object secondValue = evaluate(iterator.next(), document);
            if (!(firstValue instanceof List<?>)) {
                throw new MongoServerError(28689, "$arrayElemAt's first argument must be an array, but is " + describeType(firstValue));
            }
            if (!(secondValue instanceof Number)) {
                throw new MongoServerError(28690, "$arrayElemAt's second argument must be a numeric value, but is " + describeType(secondValue));
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
        Object apply(Object expressionValue, Document document) {
            Object value = evaluate(expressionValue, document);
            if (value == null) {
                return null;
            } else if (value instanceof Double) {
                long ceilValue = (long) Math.ceil(((Double) value).doubleValue());
                if (ceilValue <= Integer.MAX_VALUE && ceilValue >= Integer.MIN_VALUE) {
                    return Math.toIntExact(ceilValue);
                } else {
                    return ceilValue;
                }
            } else if (value instanceof Long || value instanceof Integer) {
                return value;
            } else {
                throw new MongoServerError(28765, "$ceil only supports numeric types, not " + value.getClass());
            }
        }
    },

    $cmp {
        @Override
        Object apply(Object expressionValue, Document document) {
            return Expression.evaluateComparison(expressionValue, document, "$cmp");
        }
    },

    $concat {
        @Override
        Object apply(Object expressionValue, Document document) {
            Object value = evaluate(expressionValue, document);
            if (value == null) {
                return null;
            }

            if (!(value instanceof Collection<?>)) {
                throw new MongoServerError(16702, "$concat only supports strings, not " + describeType(value));
            }

            Collection<?> collection = (Collection<?>) value;
            StringBuilder result = new StringBuilder();
            for (Object v : collection) {
                Object evaluatedValue = evaluate(v, document);
                if (!(evaluatedValue instanceof String)) {
                    throw new MongoServerError(16702, "$concat only supports strings, not " + describeType(evaluatedValue));
                }
                result.append(evaluatedValue);
            }
            return result.toString();
        }
    },

    $concatArrays {
        @Override
        Object apply(Object expressionValue, Document document) {
            Object value = evaluate(expressionValue, document);
            if (value == null) {
                return null;
            }

            if (!(value instanceof Collection<?>)) {
                throw new MongoServerError(28664, "$concatArrays only supports arrays, not " + describeType(value));
            }

            Collection<?> collection = (Collection<?>) value;
            List<Object> result = new ArrayList<>();
            for (Object v : collection) {
                Object evaluatedValue = evaluate(v, document);
                if (!(evaluatedValue instanceof Collection<?>)) {
                    throw new MongoServerError(28664, "$concatArrays only supports arrays, not " + describeType(evaluatedValue));
                }
                result.addAll((Collection<?>) evaluatedValue);
            }
            return result;
        }
    },

    $cond {
        @Override
        Object apply(Object expressionValue, Document document) {
            if (!(expressionValue instanceof Document)) {
                throw new MongoServerError(16020, "Expression $cond takes exactly 3 arguments. 1 were passed in.");
            }

            Document condDocument = (Document) expressionValue;
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

            Object ifExpression = condDocument.get("if");
            Object thenExpression = condDocument.get("then");
            Object elseExpression = condDocument.get("else");

            if (Utils.isTrue(evaluate(ifExpression, document))) {
                return evaluate(thenExpression, document);
            } else {
                return evaluate(elseExpression, document);
            }
        }
    },

    $dayOfMonth {
        @Override
        Object apply(Object expressionValue, Document document) {
            return evaluateDate(expressionValue, document, "$dayOfMonth", LocalDate::getDayOfMonth);
        }
    },

    $dayOfWeek {
        @Override
        Object apply(Object expressionValue, Document document) {
            return evaluateDate(expressionValue, document, "$dayOfWeek", date -> date.getDayOfWeek().getValue());
        }
    },

    $dayOfYear {
        @Override
        Object apply(Object expressionValue, Document document) {
            return evaluateDate(expressionValue, document, "$dayOfYear", LocalDate::getDayOfYear);
        }
    },

    $eq {
        @Override
        Object apply(Object expressionValue, Document document) {
            return evaluateComparison(expressionValue, document, "$eq", v -> v == 0);
        }
    },

    $gt {
        @Override
        Object apply(Object expressionValue, Document document) {
            return evaluateComparison(expressionValue, document, "$gt", v -> v > 0);
        }
    },

    $gte {
        @Override
        Object apply(Object expressionValue, Document document) {
            return evaluateComparison(expressionValue, document, "$gte", v -> v >= 0);
        }
    },

    $hour {
        @Override
        Object apply(Object expressionValue, Document document) {
            return evaluateTime(expressionValue, document, "$hour", LocalTime::getHour);
        }
    },

    $literal {
        @Override
        Object apply(Object expressionValue, Document document) {
            return expressionValue;
        }
    },

    $lt {
        @Override
        Object apply(Object expressionValue, Document document) {
            return evaluateComparison(expressionValue, document, "$lt", v -> v < 0);
        }
    },

    $lte {
        @Override
        Object apply(Object expressionValue, Document document) {
            return evaluateComparison(expressionValue, document, "$lte", v -> v <= 0);
        }
    },

    $minute {
        @Override
        Object apply(Object expressionValue, Document document) {
            return evaluateTime(expressionValue, document, "$minute", LocalTime::getMinute);
        }
    },

    $month {
        @Override
        Object apply(Object expressionValue, Document document) {
            return evaluateDate(expressionValue, document, "$month", date -> date.getMonth().getValue());
        }
    },

    $ne {
        @Override
        Object apply(Object expressionValue, Document document) {
            return evaluateComparison(expressionValue, document, "$ne", v -> v != 0);
        }
    },

    $second {
        @Override
        Object apply(Object expressionValue, Document document) {
            return evaluateTime(expressionValue, document, "$second", LocalTime::getSecond);
        }
    },

    $sum {
        @Override
        Object apply(Object expressionValue, Document document) {
            Object value = evaluate(expressionValue, document);
            if (value instanceof Number) {
                return value;
            } else if (value instanceof Collection) {
                Number sum = 0;
                Collection<?> collection = (Collection<?>) value;
                for (Object v : collection) {
                    Object evaluatedValue = evaluate(v, document);
                    if (evaluatedValue instanceof Number) {
                        sum = Utils.addNumbers(sum, (Number) evaluatedValue);
                    }
                }
                return sum;
            } else {
                return 0;
            }
        }
    },

    $size {
        @Override
        Object apply(Object expressionValue, Document document) {
            Object value = evaluate(expressionValue, document);
            if (!(value instanceof Collection)) {
                throw new MongoServerError(17124, "The argument to $size must be an array, but was of type: " + describeType(value));
            }
            Collection<?> collection = (Collection<?>) value;
            return collection.size();
        }
    },

    $subtract {
        @Override
        Object apply(Object expressionValue, Document document) {
            Object value = evaluate(expressionValue, document);
            if (!(value instanceof Collection)) {
                throw new MongoServerError(16020, "Expression $subtract takes exactly 2 arguments. 1 were passed in.");
            }
            Collection<?> values = (Collection<?>) value;
            if (values.size() != 2) {
                throw new MongoServerError(16020, "Expression $subtract takes exactly 2 arguments. " + values.size() + " were passed in.");
            }

            Iterator<?> iterator = values.iterator();
            Object one = evaluate(iterator.next(), document);
            Object other = evaluate(iterator.next(), document);

            if (!(one instanceof Number && other instanceof Number)) {
                throw new MongoServerError(16556,
                    "cant $subtract a " + describeType(one) + " from a " + describeType(other));
            }

            return Utils.subtractNumbers((Number) one, (Number) other);
        }
    },

    $sqrt {
        @Override
        Object apply(Object expressionValue, Document document) {
            Object value = evaluate(expressionValue, document);
            if (value == null) {
                return null;
            } else if (value instanceof Number) {
                Number number = (Number) value;
                return Math.sqrt(number.doubleValue());
            } else {
                throw new MongoServerError(28765, "$sqrt only supports numeric types, not " + value.getClass());
            }
        }
    },

    $year {
        @Override
        Object apply(Object expressionValue, Document document) {
            return evaluateDate(expressionValue, document, "$year", LocalDate::getYear);
        }
    },

    ;

    abstract Object apply(Object expressionValue, Document document);

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

    private static String describeType(Object value) {
        if (value == null) {
            return "null";
        }
        return value.getClass().getName();
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

    private static int evaluateComparison(Object expressionValue, Document document, String expressionName) {
        Object value = evaluate(expressionValue, document);
        if (!(value instanceof Collection)) {
            throw new MongoServerError(16020, "Expression " + expressionName + " takes exactly 2 arguments. 1 were passed in.");
        }
        Collection<?> parameters = (Collection<?>) value;
        if (parameters.size() != 2) {
            throw new MongoServerError(16020, "Expression " + expressionName + " takes exactly 2 arguments. " + parameters.size() + " were passed in.");
        }
        Iterator<?> iterator = parameters.iterator();
        Object firstValue = evaluate(iterator.next(), document);
        Object secondValue = evaluate(iterator.next(), document);
        return new ValueComparator().compare(firstValue, secondValue);
    }

    private static boolean evaluateComparison(Object expressionValue, Document document, String expressionName, IntPredicate comparison) {
        int comparisonResult = evaluateComparison(expressionValue, document, expressionName);
        return comparison.test(comparisonResult);
    }

    private static <T> T evaluateDateTime(Object expressionValue, Document document, String expressionName, Function<ZonedDateTime, T> dateFunction) {
        Object value = evaluate(expressionValue, document);
        if (Utils.isNullOrMissing(value)) {
            return null;
        }

        ZonedDateTime zonedDateTime = getZonedDateTime(value, expressionName);
        return dateFunction.apply(zonedDateTime);
    }

    private static <T> T evaluateDate(Object expressionValue, Document document, String expressionName, Function<LocalDate, T> dateFunction) {
        return evaluateDateTime(expressionValue, document, expressionName, zonedDateTime -> dateFunction.apply(zonedDateTime.toLocalDate()));
    }

    private static <T> T evaluateTime(Object expressionValue, Document document, String expressionName, Function<LocalTime, T> timeFunction) {
        return evaluateDateTime(expressionValue, document, expressionName, zonedDateTime -> timeFunction.apply(zonedDateTime.toLocalTime()));
    }
}
