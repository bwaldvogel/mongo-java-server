package de.bwaldvogel.mongo.backend.memory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import com.mongodb.DefaultDBEncoder;

import de.bwaldvogel.mongo.backend.Constants;
import de.bwaldvogel.mongo.backend.MongoCollection;
import de.bwaldvogel.mongo.backend.memory.index.Index;
import de.bwaldvogel.mongo.backend.memory.index.UniqueIndex;
import de.bwaldvogel.mongo.exception.CannotChangeIdOfDocumentError;
import de.bwaldvogel.mongo.exception.KeyConstraintError;
import de.bwaldvogel.mongo.exception.ModFieldNotAllowedError;
import de.bwaldvogel.mongo.exception.ModifiedFieldNameMayNotStartWithDollarError;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.wire.message.MongoDelete;
import de.bwaldvogel.mongo.wire.message.MongoInsert;
import de.bwaldvogel.mongo.wire.message.MongoUpdate;

public class MemoryCollection extends MongoCollection {

    private String collectionName;

    private List<Index> indexes = new ArrayList<Index>();

    private AtomicLong dataSize = new AtomicLong();

    private List<BSONObject> documents = new ArrayList<BSONObject>();
    private String databaseName;

    public MemoryCollection(String databaseName, String collectionName, String idField) {
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        indexes.add(new UniqueIndex(idField));
    }

    public String getFullName() {
        return databaseName + "." + getCollectionName();
    }

    public String getCollectionName() {
        return collectionName;
    }

    private static boolean matches(BSONObject query, BSONObject object) {
        for (String key : query.keySet()) {
            if (!checkMatch(query, key, object)) {
                return false;
            }
        }

        return true;
    }

    private static boolean checkMatch(BSONObject query, String key, BSONObject object) {
        Object queryValue = query.get(key);

        Object value = object.get(key);

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

        return nullAwareEquals(value, queryValue);
    }

    private static boolean checkExpressionMatch(Object value, Object expressionObject, String expression) {
        if (expression.equals("$in")) {
            Collection<?> queriedObjects = (Collection<?>) expressionObject;
            for (Object o : queriedObjects) {
                if (MongoCollection.nullAwareEquals(o, value)) {
                    return true;
                }
            }
            return false;
        } else {
            throw new UnsupportedOperationException("unsupported query expression: " + expression);
        }
    }

    private Iterable<Integer> matchDocuments(BSONObject query, Iterable<Integer> positions) {
        List<Integer> answer = new ArrayList<Integer>();
        for (Integer pos : positions) {
            BSONObject document = documents.get(pos.intValue());
            if (matches(query, document)) {
                answer.add(pos);
            }
        }
        return answer;
    }

    private Iterable<Integer> matchDocuments(BSONObject query) {
        List<Integer> answer = new ArrayList<Integer>();
        for (int i = 0; i < documents.size(); i++) {
            BSONObject document = documents.get(i);
            if (matches(query, document)) {
                answer.add(Integer.valueOf(i));
            }
        }
        return answer;
    }

    private Iterable<Integer> matchKeys(BSONObject query) {
        synchronized (indexes) {
            for (Index index : indexes) {
                if (index.canHandle(query)) {
                    return matchDocuments(query, index.getPositions(query));
                }
            }
        }

        return matchDocuments(query);
    }

    void addDocument(BSONObject document) throws KeyConstraintError {

        Integer pos = Integer.valueOf(documents.size());

        for (Index index : indexes) {
            index.checkAdd(document);
        }
        for (Index index : indexes) {
            index.add(document, pos);
        }
        dataSize.addAndGet(calculateSize(document));
        documents.add(document);
    }

    private static long calculateSize(BSONObject document) {
        return new DefaultDBEncoder().encode(document).length;
    }

    void removeDocument(BSONObject document) {
        Integer pos = null;
        for (Index index : indexes) {
            pos = index.remove(document);
        }
        if (pos == null) {
            // not found
            return;
        }
        dataSize.addAndGet(-calculateSize(document));
        documents.remove(pos.intValue());
    }

    public synchronized int getCount() {
        return documents.size();
    }

    public synchronized Iterable<BSONObject> handleQuery(BSONObject queryObject, int numberToSkip, int numberToReturn) {

        BSONObject query;
        BSONObject orderBy = null;

        if (queryObject.containsField("query")) {
            query = (BSONObject) queryObject.get("query");
            orderBy = (BSONObject) queryObject.get("orderby");
        } else if (queryObject.containsField("$query")) {
            throw new UnsupportedOperationException();
        } else {
            query = queryObject;
        }

        if (documents.isEmpty()) {
            return Collections.emptyList();
        }

        Iterable<Integer> keys = matchKeys(query);

        List<BSONObject> objs = new ArrayList<BSONObject>();
        for (Integer pos : keys) {
            if (numberToSkip > 0) {
                numberToSkip--;
                continue;
            }
            objs.add(documents.get(pos.intValue()));
            if (numberToReturn > 0 && objs.size() == numberToReturn)
                break;
        }

        if (orderBy != null && !orderBy.keySet().isEmpty()) {
            if (orderBy.keySet().iterator().next().equals("$natural")) {
                if (orderBy.get("$natural").equals(Integer.valueOf(1))) {
                    // keep it as is
                } else if (orderBy.get("$natural").equals(Integer.valueOf(-1))) {
                    Collections.reverse(objs);
                }
            } else {
                Collections.sort(objs, new BSONComparator(orderBy));
            }
        }

        return objs;
    }

    public synchronized void handleInsert(MongoInsert insert) throws MongoServerError {
        for (BSONObject document : insert.getDocuments()) {
            addDocument(document);
        }
    }

    public synchronized void handleDelete(MongoDelete delete) {
        for (BSONObject document : handleQuery(delete.getSelector(), 0, Integer.MAX_VALUE)) {
            removeDocument(document);
        }
    }

    public synchronized void handleUpdate(MongoUpdate update) throws MongoServerError {
        BSONObject newDocument = update.getUpdate();
        int n = 0;
        for (Integer position : matchKeys(update.getSelector())) {
            if (n > 0 && !update.isMulti()) {
                throw new MongoServerError(0, "no multi flag");
            }

            n++;

            BSONObject document = documents.get(position.intValue());
            for (String key : newDocument.keySet()) {
                if (key.contains("$")) {
                    modifyField(document, key, (BSONObject) newDocument.get(key));
                } else {
                    updateField(document, newDocument, key);
                }
            }
        }

        // insert?
        if (n == 0 && update.isUpsert()) {
            // TODO: check keys for $
            addDocument(newDocument);
        }
    }

    private void modifyField(BSONObject document, String modifier, BSONObject change) throws MongoServerError {
        if (modifier.equals("$set")) {
            for (String key : change.keySet()) {
                Object newValue = change.get(key);
                Object oldValue = document.get(key);

                if (nullAwareEquals(newValue, oldValue)) {
                    // no change
                    continue;
                }

                if (key.equals(Constants.ID_FIELD)) {
                    throw new ModFieldNotAllowedError(key);
                }

                document.put(key, newValue);
            }
        } else {
            throw new IllegalArgumentException("modified " + modifier + " not yet supported");
        }

    }

    private void updateField(BSONObject document, BSONObject newDocument, String key) throws MongoServerError {

        if (key.startsWith("$")) {
            throw new ModifiedFieldNameMayNotStartWithDollarError();
        }

        if (nullAwareEquals(newDocument.get(key), document.get(key))) {
            return;
        }

        if (key.equals(Constants.ID_FIELD)) {
            throw new CannotChangeIdOfDocumentError(document, newDocument);
        }

        document.put(key, newDocument.get(key));
    }

    public int getNumIndexes() {
        return indexes.size();
    }

    public long getDataSize() {
        return dataSize.get();
    }

    public long getIndexSize() {
        long indexSize = 0;
        for (Index index : indexes) {
            // actually the data size is expected. we return the count instead
            indexSize += index.getCount();
        }
        return indexSize;
    }

    public int count(BSONObject query) {
        if (query.keySet().isEmpty()) {
            return getCount();
        }

        int count = 0;
        Iterator<Integer> it = matchKeys(query).iterator();
        while (it.hasNext()) {
            it.next();
            count++;
        }
        return count;
    }

    public BSONObject getStats() {
        BSONObject response = new BasicBSONObject("ns", getFullName());
        response.put("count", Integer.valueOf(documents.size()));
        response.put("size", Long.valueOf(dataSize.get()));

        double averageSize = 0;
        if (!documents.isEmpty()) {
            averageSize = dataSize.get() / (double) documents.size();
        }
        response.put("avgObjSize", Double.valueOf(averageSize));
        response.put("storageSize", Integer.valueOf(0));
        response.put("numExtents", Integer.valueOf(0));
        response.put("nindexes", Integer.valueOf(indexes.size()));
        BSONObject indexSizes = new BasicBSONObject();
        for (Index index : indexes) {
            indexSizes.put(index.getName(), Long.valueOf(index.getDataSize()));
        }

        response.put("indexSize", indexSizes);
        response.put("ok", Integer.valueOf(1));
        return response;
    }
}
