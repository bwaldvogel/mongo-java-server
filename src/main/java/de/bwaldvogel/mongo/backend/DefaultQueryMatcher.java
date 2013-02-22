package de.bwaldvogel.mongo.backend;

import java.util.Collection;

import org.bson.BSONObject;

public class DefaultQueryMatcher implements QueryMatcher {

    @Override
    public boolean matches(BSONObject document, BSONObject query) {
        for (String key : query.keySet()) {
            if (!checkMatch(query.get(key), key, document)) {
                return false;
            }
        }

        return true;
    }

    private boolean checkMatch(Object queryValue, String key, Object document) {

        int dotPos = key.indexOf('.');
        if (dotPos > 0) {
            String mainKey = key.substring(0, dotPos);
            String subKey = key.substring(dotPos + 1);
            if (document == null || !(document instanceof BSONObject)) {
                return false;
            }

            Object subObject = ((BSONObject) document).get(mainKey);
            return checkMatch(queryValue, subKey, subObject);
        }

        if (document instanceof Collection<?>) {
            return checkMatchesAnyDocument(queryValue, key, document);
        }

        Object value = ((BSONObject) document).get(key);

        if (value instanceof Collection<?>) {
            return checkMatchesAnyValue(queryValue, value);
        }

        return checkMatchesValue(queryValue, value);
    }

    @SuppressWarnings("unchecked")
    private boolean checkMatchesAnyDocument(Object queryValue, String key, Object document) {
        for (Object object : (Collection<Object>)document) {
            if (checkMatch(queryValue, key, object)) {
                return true;
            }
        }
        return false;
    }

    private boolean checkMatchesValue(Object queryValue, Object value) {
        if (queryValue instanceof BSONObject) {
            BSONObject expressionObject = (BSONObject) queryValue;
            if (expressionObject.keySet().size() != 1) {
                throw new UnsupportedOperationException("illegal query expression: " + expressionObject);
            }

            String expression = expressionObject.keySet().iterator().next();
            if (expression.startsWith("$")) {
                return checkExpressionMatch(value, expressionObject.get(expression), expression);
            }
        }

        return Utils.nullAwareEquals(value, queryValue);
    }

    @SuppressWarnings("unchecked")
    private boolean checkMatchesAnyValue(Object queryValue, Object values) {
        for (Object value : (Collection<Object>) values) {
            if (checkMatchesValue(queryValue, value)) {
                return true;
            }
        }
        return false;
    }

    private boolean checkExpressionMatch(Object value, Object expressionObject, String expression) {
        if (expression.equals("$in")) {
            Collection<?> queriedObjects = (Collection<?>) expressionObject;
            for (Object o : queriedObjects) {
                if (Utils.nullAwareEquals(o, value)) {
                    return true;
                }
            }
            return false;
        } else {
            throw new UnsupportedOperationException("unsupported query expression: " + expression);
        }
    }
}
