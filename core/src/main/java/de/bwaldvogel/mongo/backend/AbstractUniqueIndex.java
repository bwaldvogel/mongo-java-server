package de.bwaldvogel.mongo.backend;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import de.bwaldvogel.mongo.bson.BsonRegularExpression;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.DuplicateKeyError;
import de.bwaldvogel.mongo.exception.KeyConstraintError;

public abstract class AbstractUniqueIndex<P> extends Index<P> {

    protected AbstractUniqueIndex(List<IndexKey> keys) {
        super(keys);
    }

    protected abstract P removeDocument(List<Object> key);

    protected abstract boolean containsKey(List<Object> key);

    protected abstract boolean putKeyPosition(List<Object> key, P position);

    protected abstract Iterable<Entry<List<Object>, P>> getIterable();

    protected abstract P getPosition(List<Object> key);

    @Override
    public synchronized P remove(Document document) {
        List<Object> key = getKeyValue(document);
        return removeDocument(key);
    }

    @Override
    public synchronized void checkAdd(Document document) {
        if (hasNoValueForKeys(document)) {
            return;
        }

        List<Object> key = getKeyValue(document);
        if (containsKey(key)) {
            throw new DuplicateKeyError(this, key);
        }
    }

    @Override
    public synchronized void add(Document document, P position) {
        checkAdd(document);
        if (hasNoValueForKeys(document)) {
            return;
        }
        List<Object> key = getKeyValue(document);
        boolean added = putKeyPosition(key, position);
        if (!added) {
            throw new IllegalStateException("Position " + position + " already exists. Concurrency issue?");
        }
    }

    private boolean hasNoValueForKeys(Document document) {
        for (String key : keys()) {
            if (Utils.hasSubdocumentValue(document, key)) {
                return false;
            }
        }
        return true;
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
        // no change necessary
    }

    @Override
    public synchronized boolean canHandle(Document query) {

        if (!query.keySet().equals(keySet())) {
            return false;
        }

        for (String key : keys()) {
            Object queryValue = query.get(key);
            if (queryValue instanceof Document) {
                for (String queriedKeys : ((Document) queryValue).keySet()) {
                    if (isInQuery(queriedKeys)) {
                        // okay
                    } else if (queriedKeys.startsWith("$")) {
                        // not yet supported
                        return false;
                    }
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
        List<Object> queriedKeys = getQueriedKeys(query);

        for (Object queriedKey : queriedKeys) {
            if (queriedKey instanceof Document) {
                if (isCompoundIndex()) {
                    throw new UnsupportedOperationException("Not yet implemented");
                }
                Document keyObj = (Document) queriedKey;
                if (Utils.containsQueryExpression(keyObj)) {
                    if (keyObj.keySet().size() != 1) {
                        throw new UnsupportedOperationException("illegal query key: " + queriedKeys);
                    }

                    String expression = keyObj.keySet().iterator().next();
                    if (expression.startsWith("$")) {
                        return getPositionsForExpression(keyObj, expression);
                    }
                }
            } else if (queriedKey instanceof BsonRegularExpression) {
                if (isCompoundIndex()) {
                    throw new UnsupportedOperationException("Not yet implemented");
                }
                List<P> positions = new ArrayList<>();
                for (Entry<List<Object>, P> entry : getIterable()) {
                    Object obj = entry.getKey();
                    Matcher matcher = ((BsonRegularExpression) queriedKey).matcher(obj.toString());
                    if (matcher.find()) {
                        positions.add(entry.getValue());
                    }
                }
                return positions;
            }
        }

        P position = getPosition(queriedKeys);
        if (position == null) {
            return Collections.emptyList();
        }
        return Collections.singletonList(position);
    }

    private List<Object> getQueriedKeys(Document query) {
        return keys().stream()
            .map(query::get)
            .map(Utils::normalizeValue)
            .collect(Collectors.toList());
    }

    private boolean nullAwareEqualsKeys(Document oldDocument, Document newDocument) {
        Object oldKey = getKeyValue(oldDocument);
        Object newKey = getKeyValue(newDocument);
        return Utils.nullAwareEquals(oldKey, newKey);
    }

    private Iterable<P> getPositionsForExpression(Document keyObj, String operator) {
        if (isInQuery(operator)) {
            Collection<?> queriedObjects = new TreeSet<Object>((Collection<?>) keyObj.get(operator));
            List<P> allKeys = new ArrayList<>();
            for (Object object : queriedObjects) {
                Object keyValue = Utils.normalizeValue(object);
                P key = getPosition(new ArrayList<>(Collections.singletonList(keyValue)));
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
