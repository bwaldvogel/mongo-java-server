package de.bwaldvogel.mongo.backend.aggregation;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Map.Entry;

import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;

public class Expression {

    public static Object evaluate(Object expression, Document document) {
        if (expression instanceof String && ((String) expression).startsWith("$")) {
            String value = ((String) expression).substring(1);
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
                switch (expressionKey) {
                    case "$abs":
                        return evaluateAbsValue(expressionValue, document);
                    case "$add":
                        return evaluateAddValue(expressionValue, document);
                    case "$allElementsTrue":
                        return evaluateAllElementsTrue(expressionValue, document);
                    case "$sum":
                        return evaluateSumValue(expressionValue, document);
                    case "$subtract":
                        return evaluateSubtractValue(expressionValue, document);
                    case "$year":
                        return evaluateYearValue(expressionValue, document);
                    case "$dayOfYear":
                        return evaluateDayOfYearValue(expressionValue, document);
                    case "$ceil":
                        return evaluateCeilValue(expressionValue, document);
                    case "$literal":
                        return expressionValue;
                    default:
                        throw new MongoServerError(168, "InvalidPipelineOperator", "Unrecognized expression '" + expressionKey + "'");
                }
            } else {
                result.put(expressionKey, evaluate(expressionValue, document));
            }
        }
        return result;
    }

    private static Boolean evaluateAllElementsTrue(Object expressionValue, Document document) {
        Object value = evaluate(expressionValue, document);
        if (value == null) {
            throw new MongoServerError(17040, "$allElementsTrue's argument must be an array, but is null");
        }
        if (!(value instanceof Collection)) {
            throw new MongoServerError(17040, "$allElementsTrue's argument must be an array, but is " + value.getClass().getName());
        }
        Collection<?> collection = (Collection<?>) value;
        if (collection.size() != 1) {
            throw new MongoServerError(16020, "Expression $allElementsTrue takes exactly 1 arguments. " + collection.size() + " were passed in.");
        }
        Object valueInCollection = collection.iterator().next();
        if (valueInCollection == null) {
            throw new MongoServerError(17040, "$allElementsTrue's argument must be an array, but is null");
        }
        if (!(valueInCollection instanceof Collection)) {
            throw new MongoServerError(17040, "$allElementsTrue's argument must be an array, but is " + value.getClass().getName());
        }
        Collection<?> collectionInCollection = (Collection<?>) valueInCollection;
        for (Object v : collectionInCollection) {
            if (!Utils.isTrue(v)) {
                return false;
            }
        }
        return true;
    }

    private static Number evaluateAbsValue(Object expressionValue, Document document) {
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

    private static Number evaluateCeilValue(Object expressionValue, Document document) {
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
            return (Number) value;
        } else {
            throw new MongoServerError(28765, "$ceil only supports numeric types, not " + value.getClass());
        }
    }

    private static Number evaluateSumValue(Object expressionValue, Document document) {
        Object value = evaluate(expressionValue, document);
        if (value instanceof Number) {
            return (Number) value;
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

    private static Number evaluateAddValue(Object expressionValue, Document document) {
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
                "cant $add a " + one.getClass().getName() + " and a " + other.getClass().getName());
        }

        return Utils.addNumbers((Number) one, (Number) other);
    }

    private static Number evaluateSubtractValue(Object expressionValue, Document document) {
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
                "cant $subtract a " + one.getClass().getName() + " from a " + other.getClass().getName());
        }

        return Utils.subtractNumbers((Number) one, (Number) other);
    }

    private static Integer evaluateYearValue(Object expressionValue, Document document) {
        Object value = evaluate(expressionValue, document);
        if (value == null) {
            return null;
        }

        ZonedDateTime zonedDateTime = getZonedDateTime(value);
        return zonedDateTime.toLocalDate().getYear();
    }

    private static Integer evaluateDayOfYearValue(Object expressionValue, Document document) {
        Object value = evaluate(expressionValue, document);
        if (value == null) {
            return null;
        }

        ZonedDateTime zonedDateTime = getZonedDateTime(value);
        return zonedDateTime.toLocalDate().getDayOfYear();
    }

    private static ZonedDateTime getZonedDateTime(Object value) {
        if (!(value instanceof Date)) {
            throw new MongoServerError(16006, "can't convert from " + value.getClass() + " to Date");
        }

        Instant instant = ((Date) value).toInstant();
        return ZonedDateTime.ofInstant(instant, ZoneId.systemDefault());
    }
}
