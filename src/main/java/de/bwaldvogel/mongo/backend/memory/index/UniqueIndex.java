package de.bwaldvogel.mongo.backend.memory.index;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.bson.BSONObject;

import de.bwaldvogel.mongo.backend.Constants;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.exception.DuplicateKeyError;
import de.bwaldvogel.mongo.exception.KeyConstraintError;
import de.bwaldvogel.mongo.exception.MongoServerError;

public class UniqueIndex extends Index {

    private final String key;
    private Map<Object, Integer> index = new HashMap<Object, Integer>();

    public UniqueIndex(String key) {
        super(determineName(key, true));
        this.key = key;
    }

    private static String determineName(String key, boolean ascending) {
        if (key.equals(Constants.ID_FIELD)) {
            return Constants.ID_INDEX_NAME;
        } else {
            return key + "_" + (ascending ? "1" : "-1");
        }
    }

    @Override
    protected Object getKeyValue(BSONObject document) {
        return Utils.normalizeValue(document.get(key));
    }

    @Override
    public synchronized Integer remove(BSONObject document) {
        Object value = getKeyValue(document);
        return index.remove(value);
    }

    @Override
    public synchronized void checkAdd(BSONObject document) throws KeyConstraintError {
        Object value = getKeyValue(document);
        if (value == null)
            return;

        if (index.containsKey(value)) {
            throw new DuplicateKeyError(this, value);
        }
    }

    @Override
    public synchronized void add(BSONObject document, Integer position) throws KeyConstraintError {
        checkAdd(document);
        Object value = getKeyValue(document);
        if (value != null) {
            index.put(value, position);
        }
    }

    @Override
    public void checkUpdate(BSONObject oldDocument, BSONObject newDocument) throws MongoServerError {
        if (nullAwareEqualsKeys(oldDocument, newDocument)) {
            return;
        }
        checkAdd(newDocument);
    }

    private boolean nullAwareEqualsKeys(BSONObject oldDocument, BSONObject newDocument) {
        Object oldKey = getKeyValue(oldDocument);
        Object newKey = getKeyValue(newDocument);
        return Utils.nullAwareEquals(oldKey, newKey);
    }

    @Override
    public void update(BSONObject oldDocument, BSONObject newDocument, Integer position) throws KeyConstraintError {
        if (nullAwareEqualsKeys(oldDocument, newDocument)) {
            return;
        }
        synchronized (this) {
            remove(oldDocument);
            add(newDocument, position);
        }
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
    public synchronized Iterable<Integer> getPositions(BSONObject query) {
        Object key = getKeyValue(query);

        if (key instanceof BSONObject) {
            BSONObject keyObj = (BSONObject) key;
            if (keyObj.keySet().size() != 1) {
                throw new UnsupportedOperationException("illegal query key: " + key);
            }

            String expression = keyObj.keySet().iterator().next();
            if (expression.startsWith("$")) {
                return getPositionsForExpression(keyObj, expression);
            }
        }

        Integer object = index.get(key);
        if (object == null) {
            return Collections.emptyList();
        }
        return Collections.singletonList(object);
    }

    private Iterable<Integer> getPositionsForExpression(BSONObject keyObj, String operator) {
        if (operator.equals("$in")) {
            Collection<?> queriedObjects = new TreeSet<Object>((Collection<?>) keyObj.get(operator));
            List<Integer> allPositions = new ArrayList<Integer>();
            for (Object object : queriedObjects) {
                Object value = Utils.normalizeValue(object);
                Integer pos = index.get(value);
                if (pos != null) {
                    allPositions.add(pos);
                }
            }

            return allPositions;
        } else {
            throw new UnsupportedOperationException("unsupported query expression: " + operator);
        }
    }

    @Override
    public long getCount() {
        return index.size();
    }

    @Override
    public long getDataSize() {
        return getCount(); // TODO
    }
}
