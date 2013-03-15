package de.bwaldvogel.mongo.backend.memory.index;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.bson.BSONObject;

import de.bwaldvogel.mongo.backend.Constants;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.exception.DuplicateKeyError;
import de.bwaldvogel.mongo.exception.KeyConstraintError;
import de.bwaldvogel.mongo.exception.MongoServerError;

public class UniqueIndex extends Index {

    private Map<Object, Integer> index = new HashMap<Object, Integer>();

    private final boolean ascending;
    private final String key;

    public UniqueIndex(String key, boolean ascending) {
        super();
        this.key = key;
        this.ascending = ascending;
    }

    public String getName() {
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
    public synchronized Iterable<Integer> getPositions(BSONObject query) {
        Object keyValue = getKeyValue(query);

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
            List<Integer> positions = new ArrayList<Integer>();
            for (Entry<Object, Integer> entry : index.entrySet()) {
                Object obj = entry.getKey();
                Matcher matcher = ((Pattern) keyValue).matcher(obj.toString());
                if (matcher.find()) {
                    positions.add(entry.getValue());
                }
            }
            return positions;
        }

        Integer object = index.get(keyValue);
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
