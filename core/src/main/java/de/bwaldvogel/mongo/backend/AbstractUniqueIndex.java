package de.bwaldvogel.mongo.backend;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.bwaldvogel.mongo.bson.Document;
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
    public synchronized KEY remove(Document document) {
        Object value = getKeyValue(document);
        return removeDocument(value);
    }

    @Override
    public synchronized void checkAdd(Document document) throws MongoServerError {
        if (!Utils.hasSubdocumentValue(document, key)) {
            return;
        }

        Object keyValue = getKeyValue(document);
        if (containsKeyValue(keyValue)) {
            throw new DuplicateKeyError(this, keyValue);
        }
    }

    @Override
    public synchronized void add(Document document, KEY key) throws MongoServerError {
        checkAdd(document);
        if (!Utils.hasSubdocumentValue(document, this.key)) {
            return;
        }
        Object keyValue = getKeyValue(document);
        putKeyValue(keyValue, key);
    }

    @Override
    public void checkUpdate(Document oldDocument, Document newDocument) throws MongoServerError {
        if (nullAwareEqualsKeys(oldDocument, newDocument)) {
            return;
        }
        checkAdd(newDocument);
    }

    @Override
    public void updateInPlace(Document oldDocument, Document newDocument) throws KeyConstraintError {
        if (nullAwareEqualsKeys(oldDocument, newDocument)) {
            return;
        }
        // no change necessary
    }

    @Override
    public synchronized boolean canHandle(Document query) {

        if (!query.keySet().equals(Collections.singleton(key))) {
            return false;
        }

        Object queryValue = query.get(key);
        if (queryValue instanceof Document) {
            for (String key : ((Document) queryValue).keySet()) {
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
    public synchronized Iterable<KEY> getKeys(Document query) {
        // Do not use getKeyValue, it's only valid for document.
        Object keyValue = Utils.normalizeValue(query.get(key));

        if (keyValue instanceof Document) {
            Document keyObj = (Document) keyValue;
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
            List<KEY> positions = new ArrayList<>();
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

    private boolean nullAwareEqualsKeys(Document oldDocument, Document newDocument) {
        Object oldKey = getKeyValue(oldDocument);
        Object newKey = getKeyValue(newDocument);
        return Utils.nullAwareEquals(oldKey, newKey);
    }

    private Iterable<KEY> getPositionsForExpression(Document keyObj, String operator) {
        if (operator.equals("$in")) {
            Collection<?> queriedObjects = new TreeSet<Object>((Collection<?>) keyObj.get(operator));
            List<KEY> allKeys = new ArrayList<>();
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
