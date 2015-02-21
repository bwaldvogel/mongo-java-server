package de.bwaldvogel.mongo.backend;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.bson.BSONObject;

import de.bwaldvogel.mongo.exception.DuplicateKeyError;
import de.bwaldvogel.mongo.exception.KeyConstraintError;
import de.bwaldvogel.mongo.exception.MongoServerError;

public abstract class AbstractUniqueIndex<KEY> extends Index<KEY> {

    protected AbstractUniqueIndex(String key, boolean ascending) {
        super(key, ascending);
    }

    protected abstract KEY removeDocument(Object keyValue);

    protected abstract boolean containsKeyValue(Object keyValue);

    protected abstract void putKeyValue(Object keyValue, KEY key);

    protected abstract Iterable<Entry<Object, KEY>> getIterable();

    protected abstract KEY getKey(Object keyValue);

    @Override
    public synchronized KEY remove(BSONObject document) {
        Object value = getKeyValue(document);
        return removeDocument(value);
    }

    @Override
    public synchronized void checkAdd(BSONObject document) throws KeyConstraintError {
        Object keyValue = getKeyValue(document);
        if (keyValue == null) {
            return;
        }

        if (containsKeyValue(keyValue)) {
            throw new DuplicateKeyError(this, keyValue);
        }
    }

    @Override
    public synchronized void add(BSONObject document, KEY key) throws KeyConstraintError {
        checkAdd(document);
        Object keyValue = getKeyValue(document);
        if (keyValue != null) {
            putKeyValue(keyValue, key);
        }
    }

    @Override
    public void checkUpdate(BSONObject oldDocument, BSONObject newDocument) throws MongoServerError {
        if (nullAwareEqualsKeys(oldDocument, newDocument)) {
            return;
        }
        checkAdd(newDocument);
    }

    @Override
    public void updateInPlace(BSONObject oldDocument, BSONObject newDocument) throws KeyConstraintError {
        if (nullAwareEqualsKeys(oldDocument, newDocument)) {
            return;
        }
        // no change necessary
    }

    @Override
    public synchronized boolean canHandle(BSONObject query) {

        if (!query.keySet().equals(Collections.singleton(key))) {
            return false;
        }

        Object queryValue = query.get(key);
        if (queryValue instanceof BSONObject) {
            for (String key : ((BSONObject) queryValue).keySet()) {
                if (key.equals("$in")) {
                    // okay
                } else if (key.startsWith("$")) {
                    // not yet supported
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public synchronized Iterable<KEY> getKeys(BSONObject query) {
        // Do not use getKeyValue, it's only valid for document.
        Object keyValue = Utils.normalizeValue(query.get(key));

        if (keyValue instanceof BSONObject) {
            BSONObject keyObj = (BSONObject) keyValue;
            if (Utils.containsQueryExpression(keyObj)) {
                if (keyObj.keySet().size() != 1) {
                    throw new UnsupportedOperationException("illegal query key: " + keyValue);
                }

                String expression = keyObj.keySet().iterator().next();
                if (expression.startsWith("$")) {
                    return getPositionsForExpression(keyObj, expression);
                }
            }
        } else if (keyValue instanceof Pattern) {
            List<KEY> positions = new ArrayList<KEY>();
            for (Entry<Object, KEY> entry : getIterable()) {
                Object obj = entry.getKey();
                Matcher matcher = ((Pattern) keyValue).matcher(obj.toString());
                if (matcher.find()) {
                    positions.add(entry.getValue());
                }
            }
            return positions;
        }

        KEY key = getKey(keyValue);
        if (key == null) {
            return Collections.emptyList();
        }
        return Collections.singletonList(key);
    }

    private boolean nullAwareEqualsKeys(BSONObject oldDocument, BSONObject newDocument) {
        Object oldKey = getKeyValue(oldDocument);
        Object newKey = getKeyValue(newDocument);
        return Utils.nullAwareEquals(oldKey, newKey);
    }

    private Iterable<KEY> getPositionsForExpression(BSONObject keyObj, String operator) {
        if (operator.equals("$in")) {
            Collection<?> queriedObjects = new TreeSet<Object>((Collection<?>) keyObj.get(operator));
            List<KEY> allKeys = new ArrayList<KEY>();
            for (Object object : queriedObjects) {
                Object keyValue = Utils.normalizeValue(object);
                KEY key = getKey(keyValue);
                if (key != null) {
                    allKeys.add(key);
                }
            }

            return allKeys;
        } else {
            throw new UnsupportedOperationException("unsupported query expression: " + operator);
        }
    }

}
