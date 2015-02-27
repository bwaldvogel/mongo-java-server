package de.bwaldvogel.mongo.backend;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.exception.MongoServerException;

public class DefaultQueryMatcher implements QueryMatcher {

    private ValueComparator comparator = new ValueComparator();
    private Integer lastPosition;

    @Override
    public boolean matches(BSONObject document, BSONObject query) throws MongoServerException {
        for (String key : query.keySet()) {
            if (!checkMatch(query.get(key), key, document)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public synchronized Integer matchPosition(BSONObject document, BSONObject query) throws MongoServerException {
        lastPosition = null;
        for (String key : query.keySet()) {
            if (!checkMatch(query.get(key), key, document)) {
                return null;
            }
        }

        return lastPosition;
    }

    private List<String> splitKey(String key) throws MongoServerException {
        List<String> keys = Arrays.asList(key.split("\\."));
        for (String subKey : keys) {
            if (subKey.isEmpty()) {
                throw new MongoServerException("illegal key: " + key);
            }
        }
        return keys;
    }

    private boolean checkMatch(Object queryValue, String key, Object document) throws MongoServerException {
        return checkMatch(queryValue, splitKey(key), document);
    }

    private boolean checkMatch(Object queryValue, List<String> keys, Object document) throws MongoServerException {

        if (keys.isEmpty()) {
            throw new MongoServerException("illegal keys: " + keys);
        }

        if (document == null) {
            return false;
        }

        String firstKey = keys.get(0);
        List<String> subKeys = Collections.emptyList();
        if (keys.size() > 1) {
            subKeys = keys.subList(1, keys.size());
        }

        if (firstKey.startsWith("$")) {
            if (firstKey.equals("$and") || firstKey.equals("$or") || firstKey.equals("$nor")) {
                return checkMatchAndOrNor(queryValue, firstKey, document);
            }
        }

        if (document instanceof List<?>) {
            if (firstKey.matches("\\d+")) {
                Object listValue = Utils.getListSafe(document, firstKey);
                if (subKeys.isEmpty()) {
                    return checkMatchesValue(queryValue, listValue, listValue != null);
                } else {
                    return checkMatch(queryValue, subKeys, listValue);
                }
            }

            // handle $all
            if (queryValue instanceof BSONObject && ((BSONObject) queryValue).keySet().contains("$all")) {
                // clone first
                queryValue = new BasicBSONObject(((BSONObject) queryValue).toMap());
                Object allQuery = ((BSONObject) queryValue).removeField("$all");
                if (!checkMatchesAllDocuments(allQuery, keys, document)) {
                    return false;
                }
                // continue matching the remainder of queryValue
            }

            return checkMatchesAnyDocument(queryValue, keys, document);
        }

        if (!subKeys.isEmpty()) {
            Object subObject = Utils.getListSafe(document, firstKey);
            return checkMatch(queryValue, subKeys, subObject);
        }

        if (!(document instanceof BSONObject)) {
            return false;
        }

        Object value = ((BSONObject) document).get(firstKey);
        boolean valueExists = ((BSONObject) document).containsField(firstKey);

        if (value instanceof Collection<?>) {
            if (queryValue instanceof BSONObject) {
                Set<String> keySet = ((BSONObject) queryValue).keySet();

                // clone first
                BSONObject queryValueClone = new BasicBSONObject(((BSONObject) queryValue).toMap());

                for (String queryOperator : keySet) {

                    Object subQuery = queryValueClone.removeField(queryOperator);

                    if (queryOperator.equals(QueryOperator.ALL.getValue())) {
                        if (!checkMatchesAllValues(subQuery, value)) {
                            return false;
                        }
                    } else if (queryOperator.equals(QueryOperator.IN.getValue())) {
                        final BasicBSONObject inQuery = new BasicBSONObject(queryOperator, subQuery);
                        if (!checkMatchesAnyValue(inQuery, value)) {
                            return false;
                        }
                    } else if (queryOperator.equals(QueryOperator.NOT_IN.getValue())) {
                        if (checkMatchesAllValues(subQuery, value)) {
                            return false;
                        }
                    } else if (queryOperator.equals(QueryOperator.NOT.getValue())) {
                        if (checkMatchesAnyValue(subQuery, value)) {
                            return false;
                        }
                    } else {
                        if (!checkMatchesAnyValue(queryValue, value) && !checkMatchesValue(queryValue, value, valueExists)) {
                            return false;
                        }
                    }
                }
                return true;
            }

            if (checkMatchesAnyValue(queryValue, value)) {
                return true;
            }
        }

        return checkMatchesValue(queryValue, value, valueExists);
    }

    public boolean checkMatchAndOrNor(Object queryValue, String key, Object document) throws MongoServerException {
        if (!(queryValue instanceof List<?>)) {
            throw new MongoServerError(14816, key + " expression must be a nonempty array");
        }

        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) queryValue;
        if (list.isEmpty()) {
            throw new MongoServerError(14816, key + " expression must be a nonempty array");
        }

        for (Object subqueryValue : list) {
            if (!(subqueryValue instanceof BSONObject)) {
                throw new MongoServerError(14817, key + " elements must be objects");
            }
        }

        if (key.equals("$and")) {
            for (Object subqueryValue : list) {
                if (!matches((BSONObject) document, (BSONObject) subqueryValue)) {
                    return false;
                }
            }
            return true;
        } else if (key.equals("$or")) {
            for (Object subqueryValue : list) {
                if (matches((BSONObject) document, (BSONObject) subqueryValue)) {
                    return true;
                }
            }
            return false;
        } else if (key.equals("$nor")) {
            return !checkMatchAndOrNor(queryValue, "$or", document);
        } else {
            throw new MongoServerException("illegal operation: " + key + ". must not happen");
        }
    }

    @SuppressWarnings("unchecked")
    private boolean checkMatchesAllDocuments(Object queryValue, List<String> keys, Object document)
            throws MongoServerException {
        for (Object query : (Collection<Object>) queryValue) {
            if (!checkMatchesAnyDocument(query, keys, document)) {
                return false;
            }
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    private boolean checkMatchesAnyDocument(Object queryValue, List<String> keys, Object document)
            throws MongoServerException {
        int i = 0;
        for (Object object : (Collection<Object>) document) {
            if (checkMatch(queryValue, keys, object)) {
                if (lastPosition == null) {
                    lastPosition = Integer.valueOf(i);
                }
                return true;
            }
            i++;
        }
        return false;
    }

    private boolean checkMatchesValue(Object queryValue, Object value, boolean valueExists) throws MongoServerException {
        if (queryValue instanceof BSONObject) {
            BSONObject queryObject = (BSONObject) queryValue;

            if (queryObject.containsField("$regex")) {
                String options = "";
                if (queryObject.containsField("$options")) {
                    options = queryObject.get("$options").toString();
                }

                Pattern pattern = Utils.createPattern(queryObject.get("$regex").toString(), options);
                return pattern.matcher(value.toString()).find();
            }

            for (String key : queryObject.keySet()) {
                Object querySubvalue = queryObject.get(key);
                if (key.startsWith("$")) {
                    if (!checkExpressionMatch(value, valueExists, querySubvalue, key)) {
                        return false;
                    }
                } else {
                    // the value of the query itself can be a complex query
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
    private boolean checkMatchesAllValues(Object queryValue, Object values) throws MongoServerException {

        if (!(queryValue instanceof Collection)) {
            return false;
        }

        Collection<Object> list = (Collection<Object>) values;

        for (Object query : (Collection<Object>) queryValue) {
            if (!checkMatchesAnyValue(query, list)) {
                return false;
            }
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    private boolean checkMatchesAnyValue(Object queryValue, Object values) throws MongoServerException {
        int i = 0;
        for (Object value : (Collection<Object>) values) {
            if (checkMatchesValue(queryValue, value, true)) {
                if (lastPosition == null) {
                    lastPosition = Integer.valueOf(i);
                }
                return true;
            }
            i++;
        }
        return false;
    }

    private boolean checkExpressionMatch(Object value, boolean valueExists, Object expressionValue, String operator)
            throws MongoServerException {

        final QueryOperator queryOperator;
        try {
            queryOperator = QueryOperator.fromValue(operator);
        } catch (IllegalArgumentException e) {
            throw new MongoServerError(10068, "invalid operator: " + operator);
        }

        switch (queryOperator) {
        case IN:
            Collection<?> queriedObjects = (Collection<?>) expressionValue;
            for (Object o : queriedObjects) {
                if (Utils.nullAwareEquals(o, value)) {
                    return true;
                }
            }
            return false;
        case NOT:
            return !checkMatchesValue(expressionValue, value, valueExists);
        case NOT_EQUALS:
            return !Utils.nullAwareEquals(value, expressionValue);
        case NOT_IN:
            return !checkExpressionMatch(value, valueExists, expressionValue, "$in");
        case EXISTS:
            return (valueExists == Utils.isTrue(expressionValue));
        case GREATER_THAN:
            if (!comparableTypes(value, expressionValue)) {
                return false;
            }
            return comparator.compare(value, expressionValue) > 0;
        case GREATER_THAN_OR_EQUAL:
            if (!comparableTypes(value, expressionValue)) {
                return false;
            }
            return comparator.compare(value, expressionValue) >= 0;
        case LESS_THAN:
            if (!comparableTypes(value, expressionValue)) {
                return false;
            }
            return comparator.compare(value, expressionValue) < 0;
        case LESS_THAN_OR_EQUAL:
            if (!comparableTypes(value, expressionValue)) {
                return false;
            }
            return comparator.compare(value, expressionValue) <= 0;
        case MOD: {
            if (!(value instanceof Number)) {
                return false;
            }

            @SuppressWarnings("unchecked")
            List<Number> modValue = (List<Number>) expressionValue;
            return (((Number) value).intValue() % modValue.get(0).intValue() == modValue.get(1).intValue());
        }
        case SIZE: {
            if (!(value instanceof Collection<?>) || !(expressionValue instanceof Number)) {
                return false;
            }
            int listSize = ((Collection<?>) value).size();
            double matchingSize = ((Number) expressionValue).doubleValue();
            return listSize == matchingSize;
        }
        case ALL:
            return false;

        default:
            throw new IllegalArgumentException("unhandled query operator: " + queryOperator);
        }
    }

    private boolean comparableTypes(Object value1, Object value2) {
        value1 = Utils.normalizeValue(value1);
        value2 = Utils.normalizeValue(value2);
        if (value1 == null || value2 == null) {
            return false;
        }

        return value1.getClass().equals(value2.getClass());
    }
}
