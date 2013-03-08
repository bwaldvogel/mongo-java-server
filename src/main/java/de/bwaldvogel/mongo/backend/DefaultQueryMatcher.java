package de.bwaldvogel.mongo.backend;

import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.bson.BSONObject;

import de.bwaldvogel.mongo.exception.MongoServerError;

public class DefaultQueryMatcher implements QueryMatcher {

    private ValueComparator comparator = new ValueComparator();

    @Override
    public boolean matches(BSONObject document, BSONObject query) throws MongoServerError {
        for (String key : query.keySet()) {
            if (!checkMatch(query.get(key), key, document)) {
                return false;
            }
        }

        return true;
    }

    private boolean checkMatch(Object queryValue, String key, Object document) throws MongoServerError {

        if (document == null)
            return false;

        int dotPos = key.indexOf('.');
        if (dotPos > 0) {
            String mainKey = key.substring(0, dotPos);
            String subKey = key.substring(dotPos + 1);
            Object subObject = Utils.getListSafe(document, mainKey);
            return checkMatch(queryValue, subKey, subObject);
        }

        if (document instanceof List<?>) {

            if (key.matches("\\d+")) {
                Object listValue = Utils.getListSafe(document, key);
                return checkMatchesValue(queryValue, listValue, listValue != null);
            }

            return checkMatchesAnyDocument(queryValue, key, document);
        }

        if (!(document instanceof BSONObject)) {
            return false;
        }

        Object value = ((BSONObject) document).get(key);
        boolean valueExists = ((BSONObject) document).containsField(key);

        if (value instanceof Collection<?>) {
            return checkMatchesAnyValue(queryValue, value);
        }

        return checkMatchesValue(queryValue, value, valueExists);
    }

    @SuppressWarnings("unchecked")
    private boolean checkMatchesAnyDocument(Object queryValue, String key, Object document) throws MongoServerError {
        for (Object object : (Collection<Object>) document) {
            if (checkMatch(queryValue, key, object)) {
                return true;
            }
        }
        return false;
    }

    private boolean checkMatchesValue(Object queryValue, Object value, boolean valueExists) throws MongoServerError {
        if (queryValue instanceof BSONObject) {
            BSONObject queryObject = (BSONObject) queryValue;

            for (String key : queryObject.keySet()) {
                Object querySubvalue = queryObject.get(key);
                if (key.startsWith("$")) {
                    if (!checkExpressionMatch(value, valueExists, querySubvalue, key)) {
                        return false;
                    }
                } else {
                    if (!checkMatch(querySubvalue, key, value)) {
                        return false;
                    }
                }
            }
            return true;

        }

        if (value != null && queryValue instanceof Pattern) {
            Matcher matcher = ((Pattern) queryValue).matcher(value.toString());
            return matcher.find();
        }

        return Utils.nullAwareEquals(value, queryValue);
    }

    @SuppressWarnings("unchecked")
    private boolean checkMatchesAnyValue(Object queryValue, Object values) throws MongoServerError {
        for (Object value : (Collection<Object>) values) {
            if (checkMatchesValue(queryValue, value, true)) {
                return true;
            }
        }
        return false;
    }

    private boolean checkExpressionMatch(Object value, boolean valueExists, Object expressionValue, String operator)
            throws MongoServerError {
        if (operator.equals("$in")) {
            Collection<?> queriedObjects = (Collection<?>) expressionValue;
            for (Object o : queriedObjects) {
                if (Utils.nullAwareEquals(o, value)) {
                    return true;
                }
            }
            return false;
        } else if (operator.equals("$ne")) {
            return !Utils.nullAwareEquals(value, expressionValue);
        } else if (operator.equals("$nin")) {
            return !checkExpressionMatch(value, valueExists, expressionValue, "$in");
        } else if (operator.equals("$exists")) {
            return (valueExists == Utils.isTrue(expressionValue));
        } else if (operator.equals("$gt")) {
            if (!comparableTypes(value, expressionValue))
                return false;
            return comparator.compare(value, expressionValue) > 0;
        } else if (operator.equals("$gte")) {
            if (!comparableTypes(value, expressionValue))
                return false;
            return comparator.compare(value, expressionValue) >= 0;
        } else if (operator.equals("$lt")) {
            if (!comparableTypes(value, expressionValue))
                return false;
            return comparator.compare(value, expressionValue) < 0;
        } else if (operator.equals("$lte")) {
            if (!comparableTypes(value, expressionValue))
                return false;
            return comparator.compare(value, expressionValue) <= 0;
        } else {
            throw new MongoServerError(10068, "invalid operator: " + operator);
        }
    }

    private boolean comparableTypes(Object value1, Object value2) {
        value1 = Utils.normalizeValue(value1);
        value2 = Utils.normalizeValue(value2);
        if (value1 == null || value2 == null)
            return false;

        return value1.getClass().equals(value2.getClass());
    }
}
