package de.bwaldvogel.mongo.backend.memory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import com.mongodb.BasicDBObject;

import de.bwaldvogel.mongo.backend.Constants;
import de.bwaldvogel.mongo.backend.DefaultQueryMatcher;
import de.bwaldvogel.mongo.backend.MongoCollection;
import de.bwaldvogel.mongo.backend.QueryMatcher;
import de.bwaldvogel.mongo.backend.Utils;
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

    private QueryMatcher matcher = new DefaultQueryMatcher();

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
            if (matcher.matches(document, query)) {
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

                if (key.equals(Constants.ID_FIELD)) {
                    throw new ModFieldNotAllowedError(key);
                }

                document.put(key, newValue);
            }
        } else if (modifier.equals("$push")) {
            // http://docs.mongodb.org/manual/reference/operator/push/
            if (change.keySet().size() != 1) {
                throw new UnsupportedOperationException();
            }
            String key = change.keySet().iterator().next();

            Object value = document.get(key);
            List<Object> list;
            if (value == null) {
                list = new ArrayList<Object>();
            } else if (value instanceof List<?>) {
                list = Utils.asList(value);
            } else {
                throw new UnsupportedOperationException();
            }

            list.add(change.get(key));
            document.put(key, list);
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
                throw new ModifiedFieldNameMayNotStartWithDollarError();
            }
        }

        if (newDocument.equals(oldDocument)) {
            return;
        }

        Object oldId = oldDocument.get(Constants.ID_FIELD);
        Object newId = newDocument.get(Constants.ID_FIELD);

        if (newId != null && !Utils.nullAwareEquals(oldId, newId)) {
            throw new CannotChangeIdOfDocumentError(oldDocument, newDocument);
        }

        if (newId == null) {
            if (oldId == null) {
                throw new IllegalArgumentException("document to update has no _id: " + oldDocument);
            }
            newDocument.put(Constants.ID_FIELD, oldId);
        }

        oldDocument.putAll(newDocument);
    }

    private BSONObject calculateUpdateDocument(BSONObject oldDocument, BSONObject update) throws MongoServerError {

        int numStartsWithDollar = 0;
        for (String key : update.keySet()) {
            if (key.startsWith("$")) {
                numStartsWithDollar++;
            }
        }

        BSONObject newDocument = new BasicDBObject(Constants.ID_FIELD, oldDocument.get(Constants.ID_FIELD));

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

    void addDocument(BSONObject document) throws KeyConstraintError {

        Integer pos = Integer.valueOf(documents.size());

        for (Index index : indexes) {
            index.checkAdd(document);
        }
        for (Index index : indexes) {
            index.add(document, pos);
        }
        dataSize.addAndGet(Utils.calculateSize(document));
        documents.add(document);
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
        dataSize.addAndGet(-Utils.calculateSize(document));
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
            BSONObject document = new BasicBSONObject();
            for (String key : selector.keySet()) {
                if (!key.startsWith("$")) {
                    document.put(key, selector.get(key));
                }
            }

            BSONObject newDocument = calculateUpdateDocument(document, updateQuery);
            addDocument(newDocument);
        }
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
