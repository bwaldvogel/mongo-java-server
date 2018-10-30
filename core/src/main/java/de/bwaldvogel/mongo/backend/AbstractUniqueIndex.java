package de.bwaldvogel.mongo.backend;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.regex.Matcher;

import de.bwaldvogel.mongo.bson.BsonRegularExpression;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.DuplicateKeyError;
import de.bwaldvogel.mongo.exception.KeyConstraintError;

public abstract class AbstractUniqueIndex<P> extends Index<P> {

    protected AbstractUniqueIndex(String key, boolean ascending) {
        super(key, ascending);
    }

    protected abstract P removeDocument(Object key);

    protected abstract boolean containsKey(Object key);

    protected abstract boolean putKeyPosition(Object key, P position);

    protected abstract Iterable<Entry<Object, P>> getIterable();

    protected abstract P getPosition(Object key);

    @Override
    public synchronized P remove(Document document) {
        Object key = getKey(document);
        return removeDocument(key);
    }

    @Override
    public synchronized void checkAdd(Document document) {
        if (!Utils.hasSubdocumentValue(document, key)) {
            return;
        }

        Object key = getKey(document);
        if (containsKey(key)) {
            throw new DuplicateKeyError(this, key);
        }
    }

    @Override
    public synchronized void add(Document document, P position) {
        checkAdd(document);
        if (!Utils.hasSubdocumentValue(document, this.key)) {
            return;
        }
        Object key = getKey(document);
        boolean added = putKeyPosition(key, position);
        if (!added) {
            throw new IllegalStateException("Position " + position + " already exists. Concurrency issue?");
        }
    }

    @Override
    public void checkUpdate(Document oldDocument, Document newDocument) {
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
                if (isInQuery(key)) {
                    // okay
                } else if (key.startsWith("$")) {
                    // not yet supported
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean isInQuery(String key) {
        return key.equals(QueryOperator.IN.getValue());
    }

    @Override
    public synchronized Iterable<P> getPositions(Document query) {
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
        } else if (keyValue instanceof BsonRegularExpression) {
            List<P> positions = new ArrayList<>();
            for (Entry<Object, P> entry : getIterable()) {
                Object obj = entry.getKey();
                Matcher matcher = ((BsonRegularExpression) keyValue).matcher(obj.toString());
                if (matcher.find()) {
                    positions.add(entry.getValue());
                }
            }
            return positions;
        }

        P position = getPosition(keyValue);
        if (position == null) {
            return Collections.emptyList();
        }
        return Collections.singletonList(position);
    }

    private boolean nullAwareEqualsKeys(Document oldDocument, Document newDocument) {
        Object oldKey = getKey(oldDocument);
        Object newKey = getKey(newDocument);
        return Utils.nullAwareEquals(oldKey, newKey);
    }

    private Iterable<P> getPositionsForExpression(Document keyObj, String operator) {
        if (isInQuery(operator)) {
            Collection<?> queriedObjects = new TreeSet<Object>((Collection<?>) keyObj.get(operator));
            List<P> allKeys = new ArrayList<>();
            for (Object object : queriedObjects) {
                Object keyValue = Utils.normalizeValue(object);
                P key = getPosition(keyValue);
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
