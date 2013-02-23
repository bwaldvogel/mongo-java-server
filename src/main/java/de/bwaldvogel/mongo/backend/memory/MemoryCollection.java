package de.bwaldvogel.mongo.backend.memory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;

import de.bwaldvogel.mongo.backend.DefaultQueryMatcher;
import de.bwaldvogel.mongo.backend.DocumentComparator;
import de.bwaldvogel.mongo.backend.MongoCollection;
import de.bwaldvogel.mongo.backend.QueryMatcher;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.backend.memory.index.Index;
import de.bwaldvogel.mongo.backend.memory.index.UniqueIndex;
import de.bwaldvogel.mongo.exception.KeyConstraintError;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.wire.message.MongoDelete;
import de.bwaldvogel.mongo.wire.message.MongoInsert;
import de.bwaldvogel.mongo.wire.message.MongoUpdate;

public class MemoryCollection extends MongoCollection {

    private String collectionName;

    private List<Index> indexes = new ArrayList<Index>();

    private QueryMatcher matcher = new DefaultQueryMatcher();

    private AtomicLong dataSize = new AtomicLong();

    private List<BSONObject> documents = new ArrayList<BSONObject>();
    private Queue<Integer> emptyPositions = new LinkedList<Integer>();

    private String databaseName;

    private String idField;

    public MemoryCollection(String databaseName, String collectionName, String idField) {
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        this.idField = idField;
        indexes.add(new UniqueIndex(idField));
    }

    public String getFullName() {
        return databaseName + "." + getCollectionName();
    }

    public String getCollectionName() {
        return collectionName;
    }

    private Iterable<Integer> matchDocuments(BSONObject query, Iterable<Integer> positions) {
        List<Integer> answer = new ArrayList<Integer>();
        for (Integer pos : positions) {
            BSONObject document = documents.get(pos.intValue());
            if (matcher.matches(document, query)) {
                answer.add(pos);
            }
        }
        return answer;
    }

    private Iterable<Integer> matchDocuments(BSONObject query) {
        List<Integer> answer = new ArrayList<Integer>();
        for (int i = 0; i < documents.size(); i++) {
            BSONObject document = documents.get(i);
            if (document != null && matcher.matches(document, query)) {
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

    private void modifyField(BSONObject document, String modifier, BSONObject change) throws MongoServerError {
        if (modifier.equals("$set")) {
            for (String key : change.keySet()) {
                Object newValue = change.get(key);
                Object oldValue = document.get(key);

                if (Utils.nullAwareEquals(newValue, oldValue)) {
                    // no change
                    continue;
                }

                if (key.equals(idField)) {
                    throw new MongoServerError(10148, "Mod on " + key + " not allowed");
                }

                document.put(key, newValue);
            }
        } else if (modifier.equals("$push") || modifier.equals("$pushAll")) {
            // http://docs.mongodb.org/manual/reference/operator/push/
            for (String key : change.keySet()) {
                Object value = document.get(key);
                List<Object> list;
                if (value == null) {
                    list = new ArrayList<Object>();
                } else if (value instanceof List<?>) {
                    list = Utils.asList(value);
                } else {
                    throw new MongoServerError(10141, "Cannot apply $push/$pushAll modifier to non-array");
                }

                Object pushValue = change.get(key);
                if (modifier.equals("$pushAll")) {
                    if (!(pushValue instanceof Collection<?>)) {
                        throw new MongoServerError(10153, "Modifier $pushAll/pullAll allowed for arrays only");
                    }
                    @SuppressWarnings("unchecked")
                    Collection<Object> pushValueList = (Collection<Object>) pushValue;
                    list.addAll(pushValueList);
                } else {
                list.add(pushValue);
                }
                document.put(key, list);
            }
        } else if (modifier.equals("$inc")) {
            // http://docs.mongodb.org/manual/reference/operator/inc/
            if (change.keySet().size() != 1) {
                throw new UnsupportedOperationException();
            }
            String key = change.keySet().iterator().next();

            Object value = document.get(key);
            Number number;
            if (value == null) {
                number = Integer.valueOf(0);
            } else if (value instanceof Number) {
                number = (Number) value;
            } else {
                throw new UnsupportedOperationException();
            }

            document.put(key, Utils.addNumbers(number, (Number) change.get(key)));
        } else {
            throw new IllegalArgumentException("modified " + modifier + " not yet supported");
        }

    }

    private void applyUpdate(BSONObject oldDocument, BSONObject newDocument) throws MongoServerError {

        for (String key : newDocument.keySet()) {
            if (key.startsWith("$")) {
                throw new MongoServerError(15896, "Modified field name may not start with $");
            }
        }

        if (newDocument.equals(oldDocument)) {
            return;
        }

        Object oldId = oldDocument.get(idField);
        Object newId = newDocument.get(idField);

        if (newId != null && !Utils.nullAwareEquals(oldId, newId)) {
            oldId = new BasicBSONObject(idField, oldId);
            newId = new BasicBSONObject(idField, newId);
            throw new MongoServerError(13596, "cannot change _id of a document old:" + oldId + " new:" + newId);
        }

        if (newId == null) {
            if (oldId == null) {
                throw new IllegalArgumentException("document to update has no _id: " + oldDocument);
            }
            newDocument.put(idField, oldId);
        }

        oldDocument.putAll(newDocument);
    }

    Object deriveDocumentId(BSONObject selector) {
        Object value = selector.get(idField);
        if (value != null) {
            if (!Utils.containsQueryExpression(value)) {
                return value;
            } else {
                return deriveIdFromExpression(value);
            }
        }
        return new ObjectId();
    }

    private Object deriveIdFromExpression(Object value) {
        BSONObject expression = (BSONObject) value;
        for (String key : expression.keySet()) {
            Object expressionValue = expression.get(key);
            if (key.equals("$in")) {
                Collection<?> list = (Collection<?>) expressionValue;
                if (!list.isEmpty()) {
                    return list.iterator().next();
                }
            }
        }
        // fallback to random object id
        return new ObjectId();
    }

    private BSONObject calculateUpdateDocument(BSONObject oldDocument, BSONObject update) throws MongoServerError {

        int numStartsWithDollar = 0;
        for (String key : update.keySet()) {
            if (key.startsWith("$")) {
                numStartsWithDollar++;
            }
        }

        BSONObject newDocument = new BasicDBObject(idField, oldDocument.get(idField));

        if (numStartsWithDollar == update.keySet().size()) {
            newDocument.putAll(oldDocument);
            for (String key : update.keySet()) {
                modifyField(newDocument, key, (BSONObject) update.get(key));
            }
        } else if (numStartsWithDollar == 0) {
            applyUpdate(newDocument, update);
        } else {
            throw new UnsupportedOperationException("illegal update: " + update);
        }

        return newDocument;
    }

    synchronized void addDocument(BSONObject document) throws KeyConstraintError {

        Integer pos = emptyPositions.poll();
        if (pos == null)
            pos = Integer.valueOf(documents.size());

        for (Index index : indexes) {
            index.checkAdd(document);
        }
        for (Index index : indexes) {
            index.add(document, pos);
        }
        dataSize.addAndGet(Utils.calculateSize(document));
        if (pos == documents.size()) {
            documents.add(document);
        } else {
            documents.set(pos.intValue(), document);
        }
    }

    synchronized void removeDocument(BSONObject document) {
        Integer pos = null;
        for (Index index : indexes) {
            pos = index.remove(document);
        }
        if (pos == null) {
            // not found
            return;
        }
        dataSize.addAndGet(-Utils.calculateSize(document));
        documents.set(pos, null);
        emptyPositions.add(pos);
    }

    public synchronized int getCount() {
        return documents.size() - emptyPositions.size();
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
                Collections.sort(objs, new DocumentComparator(orderBy));
            }
        }

        return objs;
    }

    public synchronized int handleInsert(MongoInsert insert) throws MongoServerError {
        int n = 0;
        for (BSONObject document : insert.getDocuments()) {
            addDocument(document);
            n++;
        }
        return n;
    }

    public synchronized int handleDelete(MongoDelete delete) {
        int n = 0;
        for (BSONObject document : handleQuery(delete.getSelector(), 0, Integer.MAX_VALUE)) {
            removeDocument(document);
            n++;
        }
        return n;
    }

    public synchronized int handleUpdate(MongoUpdate update) throws MongoServerError {
        BSONObject updateQuery = update.getUpdate();
        int n = 0;
        BSONObject selector = update.getSelector();
        for (Integer position : matchKeys(selector)) {
            if (n > 0 && !update.isMulti()) {
                throw new MongoServerError(0, "no multi flag");
            }

            n++;

            BSONObject oldDocument = documents.get(position.intValue());
            BSONObject newDocument = calculateUpdateDocument(oldDocument, updateQuery);

            if (!newDocument.equals(oldDocument)) {
                for (Index index : indexes) {
                    index.checkUpdate(oldDocument, newDocument);
                }
                for (Index index : indexes) {
                    index.update(oldDocument, newDocument, position);
                }

                long oldSize = Utils.calculateSize(oldDocument);
                long newSize = Utils.calculateSize(newDocument);
                dataSize.addAndGet(newSize - oldSize);
                documents.set(position.intValue(), newDocument);
            }
        }

        // insert?
        if (n == 0 && update.isUpsert()) {
            handleUpsert(updateQuery, selector);
            n++;
        }

        return n;
    }

    private void handleUpsert(BSONObject updateQuery, BSONObject selector) throws MongoServerError, KeyConstraintError {
        BSONObject document = convertSelectorToDocument(selector);

        BSONObject newDocument = calculateUpdateDocument(document, updateQuery);
        if (newDocument.get(idField) == null) {
            newDocument.put(idField, deriveDocumentId(selector));
        }
        addDocument(newDocument);
    }

    /**
     * convert selector used in an upsert statement into a document
     */
    BSONObject convertSelectorToDocument(BSONObject selector) {
        BSONObject document = new BasicBSONObject();
        for (String key : selector.keySet()) {
            if (key.startsWith("$"))
                continue;

            Object value = selector.get(key);

            String[] keys = key.split("\\.");
            for (int i = keys.length - 1; i > 0; i--) {
                value = new BasicBSONObject(keys[i], value);
            }

            if (keys.length > 1) {
                key = keys[0];
            }

            if (!Utils.containsQueryExpression(value)) {
                document.put(key, value);
            }
        }
        return document;
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
