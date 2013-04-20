package de.bwaldvogel.mongo.backend.memory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;

import de.bwaldvogel.mongo.backend.DefaultQueryMatcher;
import de.bwaldvogel.mongo.backend.DocumentComparator;
import de.bwaldvogel.mongo.backend.MongoCollection;
import de.bwaldvogel.mongo.backend.QueryMatcher;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.backend.ValueComparator;
import de.bwaldvogel.mongo.backend.memory.index.Index;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.exception.MongoServerException;
import de.bwaldvogel.mongo.wire.message.MongoDelete;
import de.bwaldvogel.mongo.wire.message.MongoInsert;
import de.bwaldvogel.mongo.wire.message.MongoUpdate;

public class MemoryCollection extends MongoCollection {

    private List<Index> indexes = new ArrayList<Index>();

    private QueryMatcher matcher = new DefaultQueryMatcher();

    private AtomicLong dataSize = new AtomicLong();

    private List<BSONObject> documents = new ArrayList<BSONObject>();
    private Queue<Integer> emptyPositions = new LinkedList<Integer>();

    private String idField;

    public MemoryCollection(String databaseName, String collectionName, String idField) {
        super(databaseName, collectionName);
        this.idField = idField;
    }

    public void addIndex(Index index) {
        indexes.add(index);
    }

    private Iterable<Integer> matchDocuments(BSONObject query, Iterable<Integer> positions) throws MongoServerException {
        List<Integer> answer = new ArrayList<Integer>();
        for (Integer pos : positions) {
            BSONObject document = documents.get(pos.intValue());
            if (matcher.matches(document, query)) {
                answer.add(pos);
            }
        }
        return answer;
    }

    private Iterable<Integer> matchDocuments(BSONObject query) throws MongoServerException {
        List<Integer> answer = new ArrayList<Integer>();
        for (int i = 0; i < documents.size(); i++) {
            BSONObject document = documents.get(i);
            if (document != null && matcher.matches(document, query)) {
                answer.add(Integer.valueOf(i));
            }
        }
        return answer;
    }

    private Iterable<Integer> queryDocuments(BSONObject query) throws MongoServerException {
        synchronized (indexes) {
            for (Index index : indexes) {
                if (index.canHandle(query)) {
                    return matchDocuments(query, index.getPositions(query));
                }
            }
        }

        return matchDocuments(query);
    }

    private void changeSubdocumentValue(Object document, String key, Object newValue, Integer matchPos)
            throws MongoServerException {
        changeSubdocumentValue(document, key, newValue, new AtomicReference<Integer>(matchPos));
    }

    private void changeSubdocumentValue(Object document, String key, Object newValue, AtomicReference<Integer> matchPos)
            throws MongoServerException {
        int dotPos = key.indexOf('.');
        if (dotPos > 0) {
            String mainKey = key.substring(0, dotPos);
            String subKey = getSubkey(key, dotPos, matchPos);

            Object subObject = Utils.getListSafe(document, mainKey);
            if (subObject instanceof BSONObject || subObject instanceof List<?>) {
                changeSubdocumentValue(subObject, subKey, newValue, matchPos);
            } else {
                BSONObject obj = new BasicBSONObject();
                changeSubdocumentValue(obj, subKey, newValue, matchPos);
                Utils.setListSafe(document, mainKey, obj);
            }
        } else {
            Utils.setListSafe(document, key, newValue);
        }
    }

    protected String getSubkey(String key, int dotPos, AtomicReference<Integer> matchPos) throws MongoServerError {
        String subKey = key.substring(dotPos + 1);

        if (subKey.matches("\\$(\\..+)?")) {
            if (matchPos == null || matchPos.get() == null) {
                throw new MongoServerError(16650, //
                        "Cannot apply the positional operator without a corresponding query " //
                                + "field containing an array.");
            }
            Integer pos = matchPos.getAndSet(null);
            return subKey.replaceFirst("\\$", String.valueOf(pos));
        }
        return subKey;
    }

    private void removeSubdocumentValue(Object document, String key, Integer matchPos) throws MongoServerException {
        removeSubdocumentValue(document, key, new AtomicReference<Integer>(matchPos));
    }

    private void removeSubdocumentValue(Object document, String key, AtomicReference<Integer> matchPos)
            throws MongoServerException {
        int dotPos = key.indexOf('.');
        if (dotPos > 0) {
            String mainKey = key.substring(0, dotPos);
            String subKey = getSubkey(key, dotPos, matchPos);
            Object subObject = Utils.getListSafe(document, mainKey);
            if (subObject instanceof BSONObject || subObject instanceof List<?>) {
                removeSubdocumentValue(subObject, subKey, matchPos);
            } else {
                throw new MongoServerException("failed to remove subdocument");
            }
        } else {
            Utils.removeListSafe(document, key);
        }
    }

    private Object getSubdocumentValue(Object document, String key, Integer matchPos) throws MongoServerException {
        return getSubdocumentValue(document, key, new AtomicReference<Integer>(matchPos));
    }

    private Object getSubdocumentValue(Object document, String key, AtomicReference<Integer> matchPos)
            throws MongoServerException {
        int dotPos = key.indexOf('.');
        if (dotPos > 0) {
            String mainKey = key.substring(0, dotPos);
            String subKey = getSubkey(key, dotPos, matchPos);
            Object subObject = Utils.getListSafe(document, mainKey);
            if (subObject instanceof BSONObject || subObject instanceof List<?>) {
                return getSubdocumentValue(subObject, subKey, matchPos);
            } else {
                return null;
            }
        } else {
            return Utils.getListSafe(document, key);
        }
    }

    private void modifyField(BSONObject document, String modifier, BSONObject change, Integer matchPos, boolean isUpsert)
            throws MongoServerException {

        if (!modifier.equals("$unset")) {
            for (String key : change.keySet()) {
                if (key.startsWith("$")) {
                    throw new MongoServerError(15896, "Modified field name may not start with $");
                }
            }
        }

        if (modifier.equals("$set") || (modifier.equals("$setOnInsert") && isUpsert)) {
            for (String key : change.keySet()) {
                Object newValue = change.get(key);
                Object oldValue = getSubdocumentValue(document, key, matchPos);

                if (Utils.nullAwareEquals(newValue, oldValue)) {
                    // no change
                    continue;
                }

                assertNotKeyField(key);

                changeSubdocumentValue(document, key, newValue, matchPos);
            }
        } else if (modifier.equals("$setOnInsert")) {
            // no upsert → ignore
        } else if (modifier.equals("$unset")) {
            for (String key : change.keySet()) {
                assertNotKeyField(key);
                removeSubdocumentValue(document, key, matchPos);
            }
        } else if (modifier.equals("$push") || modifier.equals("$pushAll") || modifier.equals("$addToSet")) {
            updatePushAllAddToSet(document, modifier, change, matchPos);
        } else if (modifier.equals("$pull") || modifier.equals("$pullAll")) {
            // http://docs.mongodb.org/manual/reference/operator/pull/
            for (String key : change.keySet()) {
                Object value = getSubdocumentValue(document, key, matchPos);
                List<Object> list;
                if (value == null) {
                    return;
                } else if (value instanceof List<?>) {
                    list = Utils.asList(value);
                } else {
                    throw new MongoServerError(10142, "Cannot apply " + modifier + " modifier to non-array");
                }

                Object pushValue = change.get(key);
                if (modifier.equals("$pullAll")) {
                    if (!(pushValue instanceof Collection<?>)) {
                        throw new MongoServerError(10153, "Modifier " + modifier + " allowed for arrays only");
                    }
                    @SuppressWarnings("unchecked")
                    Collection<Object> valueList = (Collection<Object>) pushValue;
                    do {
                    } while (list.removeAll(valueList));
                } else {
                    do {
                    } while (list.remove(pushValue));
                }
                // no need to put something back
            }
        } else if (modifier.equals("$pop")) {
            for (String key : change.keySet()) {
                Object value = getSubdocumentValue(document, key, matchPos);
                List<Object> list;
                if (value == null) {
                    return;
                } else if (value instanceof List<?>) {
                    list = Utils.asList(value);
                } else {
                    throw new MongoServerError(10143, "Cannot apply " + modifier + " modifier to non-array");
                }

                Object pushValue = change.get(key);
                if (!list.isEmpty()) {
                    if (pushValue != null && Utils.normalizeValue(pushValue).equals(Double.valueOf(-1.0))) {
                        list.remove(0);
                    } else {
                        list.remove(list.size() - 1);
                    }
                }
                // no need to put something back
            }
        } else if (modifier.equals("$inc")) {
            // http://docs.mongodb.org/manual/reference/operator/inc/
            for (String key : change.keySet()) {
                assertNotKeyField(key);

                Object value = getSubdocumentValue(document, key, matchPos);
                Number number;
                if (value == null) {
                    number = Integer.valueOf(0);
                } else if (value instanceof Number) {
                    number = (Number) value;
                } else {
                    throw new MongoServerException("can not increment '" + value + "'");
                }

                changeSubdocumentValue(document, key, Utils.addNumbers(number, (Number) change.get(key)), matchPos);
            }
        } else {
            throw new MongoServerError(10147, "Invalid modifier specified: " + modifier);
        }

    }

    private void updatePushAllAddToSet(BSONObject document, String modifier, BSONObject change, Integer matchPos)
            throws MongoServerException {
        // http://docs.mongodb.org/manual/reference/operator/push/
        for (String key : change.keySet()) {
            Object value = getSubdocumentValue(document, key, matchPos);
            List<Object> list;
            if (value == null) {
                list = new ArrayList<Object>();
            } else if (value instanceof List<?>) {
                list = Utils.asList(value);
            } else {
                throw new MongoServerError(10141, "Cannot apply " + modifier + " modifier to non-array");
            }

            Object changeValue = change.get(key);
            if (modifier.equals("$pushAll")) {
                if (!(changeValue instanceof Collection<?>)) {
                    throw new MongoServerError(10153, "Modifier " + modifier + " allowed for arrays only");
                }
                @SuppressWarnings("unchecked")
                Collection<Object> valueList = (Collection<Object>) changeValue;
                list.addAll(valueList);
            } else {
                Collection<Object> pushValues = new ArrayList<Object>();
                if (changeValue instanceof BSONObject
                        && ((BSONObject) changeValue).keySet().equals(Collections.singleton("$each"))) {
                    @SuppressWarnings("unchecked")
                    Collection<Object> values = (Collection<Object>) ((BSONObject) changeValue).get("$each");
                    pushValues.addAll(values);
                } else {
                    pushValues.add(changeValue);
                }

                for (Object val : pushValues) {
                    if (modifier.equals("$push")) {
                        list.add(val);
                    } else if (modifier.equals("$addToSet")) {
                        if (!list.contains(val)) {
                            list.add(val);
                        }
                    } else {
                        throw new MongoServerException("internal server error. illegal modifier here: " + modifier);
                    }
                }
            }
            changeSubdocumentValue(document, key, list, matchPos);
        }
    }

    private void assertNotKeyField(String key) throws MongoServerError {
        if (key.equals(idField)) {
            throw new MongoServerError(10148, "Mod on " + idField + " not allowed");
        }
    }

    private void applyUpdate(BSONObject oldDocument, BSONObject newDocument) throws MongoServerException {

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

        if (newId == null && oldId != null) {
            newDocument.put(idField, oldId);
        }

        cloneInto(oldDocument, newDocument);
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

    private BSONObject calculateUpdateDocument(BSONObject oldDocument, BSONObject update, Integer matchPos,
            boolean isUpsert) throws MongoServerException {

        int numStartsWithDollar = 0;
        for (String key : update.keySet()) {
            if (key.startsWith("$")) {
                numStartsWithDollar++;
            }
        }

        BSONObject newDocument = new BasicBSONObject(idField, oldDocument.get(idField));

        if (numStartsWithDollar == update.keySet().size()) {
            cloneInto(newDocument, oldDocument);
            for (String key : update.keySet()) {
                modifyField(newDocument, key, (BSONObject) update.get(key), matchPos, isUpsert);
            }
        } else if (numStartsWithDollar == 0) {
            applyUpdate(newDocument, update);
        } else {
            throw new MongoServerException("illegal update: " + update);
        }

        return newDocument;
    }

    public synchronized void addDocument(BSONObject document) throws MongoServerException {

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

    public synchronized void removeDocument(BSONObject document) throws MongoServerException {
        Integer pos = null;

        if (!indexes.isEmpty()) {
            for (Index index : indexes) {
                pos = index.remove(document);
            }
        } else {
            int idx = documents.indexOf(document);
            if (idx >= 0) {
                pos = Integer.valueOf(idx);
            }
        }
        if (pos == null) {
            // not found
            return;
        }
        dataSize.addAndGet(-Utils.calculateSize(document));
        documents.set(pos.intValue(), null);
        emptyPositions.add(pos);
    }

    public synchronized int count() {
        return documents.size() - emptyPositions.size();
    }

    public synchronized BSONObject findAndModify(BSONObject query) throws MongoServerException {

        boolean returnNew = Utils.isTrue(query.get("new"));

        if (!query.containsField("remove") && !query.containsField("update")) {
            throw new MongoServerException("need remove or update");
        }

        BSONObject queryObject = new BasicBSONObject();

        if (query.containsField("query")) {
            queryObject.put("query", query.get("query"));
        } else {
            queryObject.put("query", new BasicBSONObject());
        }

        if (query.containsField("sort")) {
            queryObject.put("orderby", query.get("sort"));
        }

        BSONObject lastErrorObject = null;
        BSONObject returnDocument = null;
        int num = 0;
        for (BSONObject document : handleQuery(queryObject, 0, 1)) {
            num++;
            if (Utils.isTrue(query.get("remove"))) {
                removeDocument(document);
                returnDocument = document;
            } else if (query.get("update") != null) {
                BSONObject updateQuery = (BSONObject) query.get("update");

                Integer matchPos = matcher.matchPosition(document, (BSONObject) queryObject.get("query"));

                BSONObject oldDocument = updateDocument(document, updateQuery, matchPos);
                if (returnNew) {
                    returnDocument = document;
                } else {
                    returnDocument = oldDocument;
                }
                lastErrorObject = new BasicBSONObject("updatedExisting", Boolean.TRUE);
                lastErrorObject.put("n", Integer.valueOf(1));
            }
        }
        if (num == 0 && Utils.isTrue(query.get("upsert"))) {
            BSONObject selector = (BSONObject) query.get("query");
            BSONObject updateQuery = (BSONObject) query.get("update");
            BSONObject newDocument = handleUpsert(updateQuery, selector);
            if (returnNew) {
                returnDocument = newDocument;
            } else {
                returnDocument = new BasicBSONObject();
            }
            num++;
        }

        if (query.get("fields") != null) {
            BSONObject fields = (BSONObject) query.get("fields");
            returnDocument = projectDocument(returnDocument, fields);
        }

        BSONObject result = new BasicBSONObject();
        if (lastErrorObject != null) {
            result.put("lastErrorObject", lastErrorObject);
        }
        result.put("value", returnDocument);
        Utils.markOkay(result);
        return result;
    }

    private BSONObject projectDocument(BSONObject document, BSONObject fields) {
        if (document == null)
            return null;
        BSONObject newDocument = new BasicBSONObject();
        for (String key : fields.keySet()) {
            if (Utils.isTrue(fields.get(key))) {
                newDocument.put(key, document.get(key));
            }
        }

        // implicitly add _id if not mentioned
        // http://docs.mongodb.org/manual/core/read-operations/#result-projections
        if (!fields.containsField(idField)) {
            newDocument.put(idField, document.get(idField));
        }

        return newDocument;
    }

    public synchronized Iterable<BSONObject> handleQuery(BSONObject queryObject, int numberToSkip, int numberToReturn)
            throws MongoServerException {
        return handleQuery(queryObject, numberToSkip, numberToReturn, null);
    }

    public synchronized Iterable<BSONObject> handleQuery(BSONObject queryObject, int numberToSkip, int numberToReturn,
            BSONObject fieldSelector) throws MongoServerException {

        BSONObject query;
        BSONObject orderBy = null;

        if (numberToReturn < 0) {
            // actually: request to close cursor automatically
            numberToReturn = -numberToReturn;
        }

        if (queryObject.containsField("query")) {
            query = (BSONObject) queryObject.get("query");
            orderBy = (BSONObject) queryObject.get("orderby");
        } else if (queryObject.containsField("$query")) {
            query = (BSONObject) queryObject.get("$query");
            orderBy = (BSONObject) queryObject.get("$orderby");
        } else {
            query = queryObject;
        }

        if (documents.isEmpty()) {
            return Collections.emptyList();
        }

        Iterable<Integer> keys = queryDocuments(query);

        List<BSONObject> objs = new ArrayList<BSONObject>();
        for (Integer pos : keys) {
            if (numberToSkip > 0) {
                numberToSkip--;
                continue;
            }
            objs.add(documents.get(pos.intValue()));
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

        if (numberToReturn > 0 && objs.size() > numberToReturn)
            objs = objs.subList(0, numberToReturn);

        if (fieldSelector != null && !fieldSelector.keySet().isEmpty()) {
            for (int i = 0; i < objs.size(); i++) {
                objs.set(i, projectDocument(objs.get(i), fieldSelector));
            }
        }

        return objs;
    }

    public synchronized BSONObject handleDistinct(BSONObject query) throws MongoServerException {
        String key = query.get("key").toString();
        BSONObject q = (BSONObject) query.get("query");
        TreeSet<Object> values = new TreeSet<Object>(new ValueComparator());

        for (Integer pos : queryDocuments(q)) {
            BSONObject document = documents.get(pos.intValue());
            if (document.containsField(key))
                values.add(document.get(key));
        }

        BSONObject response = new BasicBSONObject("values", new ArrayList<Object>(values));
        Utils.markOkay(response);
        return response;
    }

    public synchronized int handleInsert(MongoInsert insert) throws MongoServerException {
        int n = 0;
        for (BSONObject document : insert.getDocuments()) {
            addDocument(document);
            n++;
        }
        return n;
    }

    public synchronized int handleDelete(MongoDelete delete) throws MongoServerException {
        int n = 0;
        int numToReturn = delete.isSingleRemove() ? 1 : Integer.MAX_VALUE;
        for (BSONObject document : handleQuery(delete.getSelector(), 0, numToReturn)) {
            if (n >= numToReturn) {
                throw new MongoServerException("internal error: too many elements (" + n + " >= " + numToReturn + ")");
            }
            removeDocument(document);
            n++;
        }
        return n;
    }

    public synchronized BSONObject handleUpdate(MongoUpdate update) throws MongoServerException {
        BSONObject updateQuery = update.getUpdate();
        int n = 0;
        boolean updatedExisting = false;
        BSONObject selector = update.getSelector();

        if (update.isMulti()) {
            for (String key : updateQuery.keySet()) {
                if (!key.startsWith("$")) {
                    throw new MongoServerError(10158, "multi update only works with $ operators");
                }
            }
        }

        for (Integer position : queryDocuments(selector)) {
            BSONObject document = documents.get(position.intValue());
            Integer matchPos = matcher.matchPosition(document, selector);
            updateDocument(document, updateQuery, matchPos);
            updatedExisting = true;
            n++;

            if (!update.isMulti()) {
                break;
            }
        }

        BSONObject result = new BasicBSONObject();

        // insert?
        if (n == 0 && update.isUpsert()) {
            BSONObject newDocument = handleUpsert(updateQuery, selector);
            if (!selector.containsField(idField)) {
                result.put("upserted", newDocument.get(idField));
            }
            n++;
        }

        result.put("n", Integer.valueOf(n));
        result.put("updatedExisting", Boolean.valueOf(updatedExisting));
        return result;
    }

    private BSONObject updateDocument(BSONObject document, BSONObject updateQuery, Integer matchPos)
            throws MongoServerException {
        synchronized (document) {
            // copy document
            BSONObject oldDocument = new BasicBSONObject();
            cloneInto(oldDocument, document);

            BSONObject newDocument = calculateUpdateDocument(document, updateQuery, matchPos, false);

            if (!newDocument.equals(oldDocument)) {
                for (Index index : indexes) {
                    index.checkUpdate(oldDocument, newDocument);
                }
                for (Index index : indexes) {
                    index.updateInPlace(oldDocument, newDocument);
                }

                long oldSize = Utils.calculateSize(oldDocument);
                long newSize = Utils.calculateSize(newDocument);
                dataSize.addAndGet(newSize - oldSize);

                // only keep fields that are also in the updated document
                Set<String> fields = new HashSet<String>(document.keySet());
                fields.removeAll(newDocument.keySet());
                for (String key : fields) {
                    document.removeField(key);
                }

                // update the fields
                for (String key : newDocument.keySet()) {
                    if (key.contains(".")) {
                        throw new MongoServerException(
                                "illegal field name. must not happen as it must be catched by the driver");
                    }
                    document.put(key, newDocument.get(key));
                }
            }
            return oldDocument;
        }
    }

    private void cloneInto(BSONObject targetDocument, BSONObject sourceDocument) {
        for (String key : sourceDocument.keySet()) {
            targetDocument.put(key, cloneValue(sourceDocument.get(key)));
        }
    }

    protected Object cloneValue(Object value) {
        if (value instanceof BSONObject) {
            BSONObject newValue = new BasicBSONObject();
            cloneInto(newValue, (BSONObject) value);
            return newValue;
        } else if (value instanceof List<?>) {
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) value;
            List<Object> newValue = new ArrayList<Object>();
            for (Object v : list) {
                newValue.add(cloneValue(v));
            }
            return newValue;
        } else {
            return value;
        }
    }

    private BSONObject handleUpsert(BSONObject updateQuery, BSONObject selector) throws MongoServerException {
        BSONObject document = convertSelectorToDocument(selector);

        BSONObject newDocument = calculateUpdateDocument(document, updateQuery, null, true);
        if (newDocument.get(idField) == null) {
            newDocument.put(idField, deriveDocumentId(selector));
        }
        addDocument(newDocument);
        return newDocument;
    }

    /**
     * convert selector used in an upsert statement into a document
     */
    BSONObject convertSelectorToDocument(BSONObject selector) throws MongoServerException {
        BSONObject document = new BasicBSONObject();
        for (String key : selector.keySet()) {
            if (key.startsWith("$"))
                continue;

            Object value = selector.get(key);
            if (!Utils.containsQueryExpression(value)) {
                changeSubdocumentValue(document, key, value, (AtomicReference<Integer>) null);
            }
        }
        return document;
    }

    public int getNumIndexes() {
        return indexes.size();
    }

    public int count(BSONObject query) throws MongoServerException {
        if (query.keySet().isEmpty()) {
            return count();
        }

        int count = 0;
        Iterator<Integer> it = queryDocuments(query).iterator();
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
        Utils.markOkay(response);
        return response;
    }

    public BSONObject validate() {
        BSONObject response = new BasicBSONObject("ns", getFullName());
        response.put("extentCount", Integer.valueOf(0));
        response.put("datasize", Long.valueOf(dataSize.get()));
        response.put("nrecords", Integer.valueOf(documents.size()));
        response.put("padding", Integer.valueOf(1));
        response.put("deletedCount", Integer.valueOf(emptyPositions.size()));
        response.put("deletedSize", Integer.valueOf(0));

        response.put("nIndexes", Integer.valueOf(indexes.size()));
        BSONObject keysPerIndex = new BasicBSONObject();
        for (Index index : indexes) {
            keysPerIndex.put(index.getName(), Long.valueOf(index.getCount()));
        }

        response.put("keysPerIndex", keysPerIndex);
        response.put("valid", Boolean.TRUE);
        response.put("errors", Arrays.asList());
        Utils.markOkay(response);
        return response;
    }
}
