package de.bwaldvogel.mongo.backend;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.bson.BsonTimestamp;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.ObjectId;
import de.bwaldvogel.mongo.exception.BadValueException;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.exception.MongoServerException;

public abstract class AbstractMongoCollection<P> implements MongoCollection<P> {

    private String collectionName;
    private String databaseName;
    private final List<Index<P>> indexes = new ArrayList<>();
    private final QueryMatcher matcher = new DefaultQueryMatcher();
    protected final String idField;

    protected AbstractMongoCollection(String databaseName, String collectionName, String idField) {
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        this.idField = idField;
    }

    protected boolean documentMatchesQuery(Document document, Document query) throws MongoServerException {
        return matcher.matches(document, query);
    }

    private Iterable<Document> queryDocuments(Document query, Document orderBy, int numberToSkip,
                                                int numberToReturn) throws MongoServerException {
        synchronized (indexes) {
            for (Index<P> index : indexes) {
                if (index.canHandle(query)) {
                    Iterable<P> positions = index.getPositions(query);
                    return matchDocuments(query, positions, orderBy, numberToSkip, numberToReturn);
                }
            }
        }

        return matchDocuments(query, orderBy, numberToSkip, numberToReturn);
    }

    protected void sortDocumentsInMemory(List<Document> documents, Document orderBy) {
        if (orderBy != null && !orderBy.keySet().isEmpty()) {
            if (orderBy.keySet().iterator().next().equals("$natural")) {
                int sortValue = ((Integer) orderBy.get("$natural")).intValue();
                if (sortValue == 1) {
                    // keep it as is
                } else if (sortValue == -1) {
                    Collections.reverse(documents);
                } else {
                    throw new IllegalArgumentException("Illegal sort value: " + sortValue);
                }
            } else {
                documents.sort(new DocumentComparator(orderBy));
            }
        }
    }

    protected abstract Iterable<Document> matchDocuments(Document query, Document orderBy, int numberToSkip,
                                                           int numberToReturn) throws MongoServerException;

    protected abstract Iterable<Document> matchDocuments(Document query, Iterable<P> positions, Document orderBy,
                                                         int numberToSkip, int numberToReturn) throws MongoServerException;

    protected abstract Document getDocument(P position);

    protected abstract void updateDataSize(long sizeDelta) throws MongoServerException;

    protected abstract long getDataSize() throws MongoServerException;

    protected abstract P addDocumentInternal(Document document) throws MongoServerException;

    @Override
    public synchronized void addDocument(Document document) throws MongoServerException {

        for (Index<P> index : indexes) {
            index.checkAdd(document);
        }

        P position = addDocumentInternal(document);

        for (Index<P> index : indexes) {
            index.add(document, position);
        }

        updateDataSize(Utils.calculateSize(document));
    }

    @Override
    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public String getFullName() {
        return getDatabaseName() + "." + getCollectionName();
    }

    @Override
    public String getCollectionName() {
        return collectionName;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" + getFullName() + ")";
    }

    @Override
    public void addIndex(Index<P> index) {
        indexes.add(index);
    }

    private void assertNotKeyField(String key) throws MongoServerError {
        if (key.equals(idField)) {
            throw new MongoServerError(10148, "Mod on " + idField + " not allowed");
        }
    }

    private void changeSubdocumentValue(Object document, String key, Object newValue, Integer matchPos)
            throws MongoServerException {
        changeSubdocumentValue(document, key, newValue, new AtomicReference<>(matchPos));
    }

    private void changeSubdocumentValue(Object document, String key, Object newValue, AtomicReference<Integer> matchPos)
            throws MongoServerException {
        int dotPos = key.indexOf('.');
        if (dotPos > 0) {
            String mainKey = key.substring(0, dotPos);
            String subKey = Utils.getSubkey(key, dotPos, matchPos);

            Object subObject = Utils.getFieldValueListSafe(document, mainKey);
            if (subObject instanceof Document || subObject instanceof List<?>) {
                changeSubdocumentValue(subObject, subKey, newValue, matchPos);
            } else {
                Document obj = new Document();
                changeSubdocumentValue(obj, subKey, newValue, matchPos);
                Utils.setListSafe(document, mainKey, obj);
            }
        } else {
            Utils.setListSafe(document, key, newValue);
        }
    }

    private Object removeSubdocumentValue(Object document, String key, Integer matchPos) throws MongoServerException {
        return removeSubdocumentValue(document, key, new AtomicReference<>(matchPos));
    }

    private Object removeSubdocumentValue(Object document, String key, AtomicReference<Integer> matchPos)
            throws MongoServerException {
        int dotPos = key.indexOf('.');
        if (dotPos > 0) {
            String mainKey = key.substring(0, dotPos);
            String subKey = Utils.getSubkey(key, dotPos, matchPos);
            Object subObject = Utils.getFieldValueListSafe(document, mainKey);
            if (subObject instanceof Document || subObject instanceof List<?>) {
                return removeSubdocumentValue(subObject, subKey, matchPos);
            } else {
                throw new MongoServerException("failed to remove subdocument");
            }
        } else {
            return Utils.removeListSafe(document, key);
        }
    }

    private Object getSubdocumentValue(Object document, String key, Integer matchPos) throws MongoServerException {
        return getSubdocumentValue(document, key, new AtomicReference<>(matchPos));
    }

    private Object getSubdocumentValue(Object document, String key, AtomicReference<Integer> matchPos)
            throws MongoServerException {
        int dotPos = key.indexOf('.');
        if (dotPos > 0) {
            String mainKey = key.substring(0, dotPos);
            String subKey = Utils.getSubkey(key, dotPos, matchPos);
            Object subObject = Utils.getFieldValueListSafe(document, mainKey);
            if (subObject instanceof Document || subObject instanceof List<?>) {
                return getSubdocumentValue(subObject, subKey, matchPos);
            } else {
                return null;
            }
        } else {
            return Utils.getFieldValueListSafe(document, key);
        }
    }

    private void modifyField(Document document, String modifier, Document change, Integer matchPos,
            boolean isUpsert) throws MongoServerException {

        UpdateOperator op = getUpdateOperator(modifier, change);

        switch (op) {
        case SET_ON_INSERT:
            if (!isUpsert) {
                // no upsert → ignore
                return;
            }
            //$FALL-THROUGH$
        case SET:
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
            break;

        case UNSET:
            for (String key : change.keySet()) {
                assertNotKeyField(key);
                removeSubdocumentValue(document, key, matchPos);
            }
            break;

        case PUSH:
        case PUSH_ALL:
        case ADD_TO_SET:
            updatePushAllAddToSet(document, op, change, matchPos);
            break;

        case PULL:
        case PULL_ALL:
            for (String key : change.keySet()) {
                Object value = getSubdocumentValue(document, key, matchPos);
                List<Object> list;
                if (value == null) {
                    return;
                } else if (value instanceof List<?>) {
                    list = asList(value);
                } else {
                    throw new MongoServerError(10142, "Cannot apply " + modifier + " modifier to non-array");
                }

                Object pullValue = change.get(key);
                if (modifier.equals("$pullAll")) {
                    if (!(pullValue instanceof Collection<?>)) {
                        throw new MongoServerError(10153, "Modifier " + modifier + " allowed for arrays only");
                    }
                    @SuppressWarnings("unchecked")
                    Collection<Object> valueList = (Collection<Object>) pullValue;
                    do {
                    } while (list.removeAll(valueList));
                } else {
                    Iterator<Object> it = list.iterator();
                    while (it.hasNext()) {
                        Object obj = it.next();
                        if (matcher.matchesValue(pullValue, obj)) {
                            it.remove();
                        }
                    }
                }
                // no need to put something back
            }
            break;

        case POP:
            for (String key : change.keySet()) {
                Object value = getSubdocumentValue(document, key, matchPos);
                List<Object> list;
                if (value == null) {
                    return;
                } else if (value instanceof List<?>) {
                    list = asList(value);
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
            break;

        case INC:
        case MUL:
            for (String key : change.keySet()) {
                assertNotKeyField(key);

                String operation = (op == UpdateOperator.INC) ? "increment" : "multiply";
                Object value = getSubdocumentValue(document, key, matchPos);
                Number number;
                if (value == null) {
                    number = Integer.valueOf(0);
                } else if (value instanceof Number) {
                    number = (Number) value;
                } else {
                    throw new MongoServerException("cannot " + operation + " value '" + value + "'");
                }

                Object changeObject = change.get(key);
                if (!(changeObject instanceof Number)) {
                    throw new MongoServerException("cannot " + operation + " with non-numeric value: " + change);
                }
                Number changeValue = (Number) changeObject;
                final Number newValue;
                if (op == UpdateOperator.INC) {
                    newValue = Utils.addNumbers(number, changeValue);
                } else if (op == UpdateOperator.MUL) {
                    newValue = Utils.multiplyNumbers(number, changeValue);
                } else {
                    throw new RuntimeException();
                }

                changeSubdocumentValue(document, key, newValue, matchPos);
            }
            break;

        case MIN:
        case MAX:
            Comparator<Object> comparator = new ValueComparator();
            for (String key : change.keySet()) {
                assertNotKeyField(key);

                Object newValue = change.get(key);
                Object oldValue = getSubdocumentValue(document, key, matchPos);

                int valueComparison = comparator.compare(newValue, oldValue);

                final boolean shouldChange;
                // If the field does not exists, the $min/$max operator sets the
                // field to the specified value
                if (oldValue == null && !Utils.hasSubdocumentValue(document, key)) {
                    shouldChange = true;
                } else if (op == UpdateOperator.MAX) {
                    shouldChange = valueComparison > 0;
                } else if (op == UpdateOperator.MIN) {
                    shouldChange = valueComparison < 0;
                } else {
                    throw new RuntimeException();
                }

                if (shouldChange) {
                    changeSubdocumentValue(document, key, newValue, matchPos);
                }
            }
            break;

        case CURRENT_DATE:
            for (String key : change.keySet()) {
                assertNotKeyField(key);

                Object typeSpecification = change.get(key);

                final boolean useDate;
                if (typeSpecification instanceof Boolean && Utils.isTrue(typeSpecification)) {
                    useDate = true;
                } else if (typeSpecification instanceof Document) {
                    Object type = ((Document) typeSpecification).get("$type");
                    if (type.equals("timestamp")) {
                        useDate = false;
                    } else if (type.equals("date")) {
                        useDate = true;
                    } else {
                        throw new BadValueException("The '$type' string field is required to be 'date' or 'timestamp': " + change);
                    }
                } else {
                    final String type;
                    if (typeSpecification != null) {
                        type = typeSpecification.getClass().getSimpleName();
                    } else {
                        type = "NULL";
                    }
                    throw new BadValueException(type + " is not a valid type for $currentDate." + //
                            " Please use a boolean ('true') or a $type expression ({$type: 'timestamp/date'})");
                }

                final Object newValue;
                if (useDate) {
                    newValue = new Date();
                } else {
                    newValue = new BsonTimestamp(System.currentTimeMillis());
                }

                changeSubdocumentValue(document, key, newValue, matchPos);
            }
            break;

        case RENAME:
            Map<String, String> renames = new LinkedHashMap<>();
            for (String key : change.keySet()) {
                assertNotKeyField(key);
                Object toField = change.get(key);
                if (!(toField instanceof String)) {
                    throw new BadValueException("The 'to' field for $rename must be a string: " + toField);
                }
                String newKey = (String) toField;
                assertNotKeyField(newKey);

                if (renames.containsKey(key) || renames.containsValue(key)) {
                    throw new MongoServerError(16837, "Cannot update '" + key + "' and '" + key + "' at the same time");
                }
                if (renames.containsKey(newKey) || renames.containsValue(newKey)) {
                    throw new MongoServerError(16837,
                            "Cannot update '" + newKey + "' and '" + newKey + "' at the same time");
                }

                renames.put(key, newKey);
            }

            for (Entry<String, String> entry : renames.entrySet()) {
                Object value = removeSubdocumentValue(document, entry.getKey(), matchPos);
                changeSubdocumentValue(document, entry.getValue(), value, matchPos);
            }
            break;

        default:
            throw new MongoServerError(10147, "Unsupported modifier: " + modifier);
        }
    }

    private UpdateOperator getUpdateOperator(String modifier, Document change) throws MongoServerError {
        final UpdateOperator op;
        try {
            op = UpdateOperator.fromValue(modifier);
        } catch (IllegalArgumentException e) {
            throw new MongoServerError(10147, "Invalid modifier specified: " + modifier);
        }

        if (op != UpdateOperator.UNSET) {
            for (String key : change.keySet()) {
                if (key.startsWith("$")) {
                    throw new MongoServerError(15896, "Modified field name may not start with $");
                }
            }
        }
        return op;
    }

    private void updatePushAllAddToSet(Document document, UpdateOperator updateOperator, Document change,
            Integer matchPos) throws MongoServerException {
        // http://docs.mongodb.org/manual/reference/operator/push/
        for (String key : change.keySet()) {
            Object value = getSubdocumentValue(document, key, matchPos);
            List<Object> list;
            if (value == null) {
                list = new ArrayList<>();
            } else if (value instanceof List<?>) {
                list = asList(value);
            } else {
                throw new MongoServerError(10141, "Cannot apply " + updateOperator + " modifier to non-array");
            }

            Object changeValue = change.get(key);
            if (updateOperator == UpdateOperator.PUSH_ALL) {
                if (!(changeValue instanceof Collection<?>)) {
                    throw new MongoServerError(10153, "Modifier " + updateOperator + " allowed for arrays only");
                }
                @SuppressWarnings("unchecked")
                Collection<Object> valueList = (Collection<Object>) changeValue;
                list.addAll(valueList);
            } else {
                Collection<Object> pushValues = new ArrayList<>();
                if (changeValue instanceof Document
                        && ((Document) changeValue).keySet().equals(Collections.singleton("$each"))) {
                    @SuppressWarnings("unchecked")
                    Collection<Object> values = (Collection<Object>) ((Document) changeValue).get("$each");
                    pushValues.addAll(values);
                } else {
                    pushValues.add(changeValue);
                }

                for (Object val : pushValues) {
                    if (updateOperator == UpdateOperator.PUSH) {
                        list.add(val);
                    } else if (updateOperator == UpdateOperator.ADD_TO_SET) {
                        if (!list.contains(val)) {
                            list.add(val);
                        }
                    } else {
                        throw new MongoServerException(
                                "internal server error. illegal modifier here: " + updateOperator);
                    }
                }
            }
            changeSubdocumentValue(document, key, list, matchPos);
        }
    }

    private void applyUpdate(Document oldDocument, Document newDocument) throws MongoServerException {

        if (newDocument.equals(oldDocument)) {
            return;
        }

        Object oldId = oldDocument.get(idField);
        Object newId = newDocument.get(idField);

        if (newId != null && !Utils.nullAwareEquals(oldId, newId)) {
            throw new MongoServerError(13596, "cannot change _id of a document old: " + oldId + ", new: " + newId);
        }

        if (newId == null && oldId != null) {
            newDocument.put(idField, oldId);
        }

        cloneInto(oldDocument, newDocument);
    }

    Object deriveDocumentId(Document selector) {
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
        Document expression = (Document) value;
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

    private Document calculateUpdateDocument(Document oldDocument, Document update, Integer matchPos,
            boolean isUpsert) throws MongoServerException {

        int numStartsWithDollar = 0;
        for (String key : update.keySet()) {
            if (key.startsWith("$")) {
                numStartsWithDollar++;
            }
        }

        Document newDocument = new Document(idField, oldDocument.get(idField));

        if (numStartsWithDollar == update.keySet().size()) {
            cloneInto(newDocument, oldDocument);
            for (String key : update.keySet()) {
                modifyField(newDocument, key, (Document) update.get(key), matchPos, isUpsert);
            }
        } else if (numStartsWithDollar == 0) {
            applyUpdate(newDocument, update);
        } else {
            throw new MongoServerException("illegal update: " + update);
        }

        return newDocument;
    }

    @Override
    public synchronized Document findAndModify(Document query) throws MongoServerException {

        boolean returnNew = Utils.isTrue(query.get("new"));

        if (!query.containsKey("remove") && !query.containsKey("update")) {
            throw new MongoServerException("need remove or update");
        }

        Document queryObject = new Document();

        if (query.containsKey("query")) {
            queryObject.put("query", query.get("query"));
        } else {
            queryObject.put("query", new Document());
        }

        if (query.containsKey("sort")) {
            queryObject.put("orderby", query.get("sort"));
        }

        Document lastErrorObject = null;
        Document returnDocument = null;
        int num = 0;
        for (Document document : handleQuery(queryObject, 0, 1)) {
            num++;
            if (Utils.isTrue(query.get("remove"))) {
                removeDocument(document);
                returnDocument = document;
            } else if (query.get("update") != null) {
                Document updateQuery = (Document) query.get("update");

                Integer matchPos = matcher.matchPosition(document, (Document) queryObject.get("query"));

                Document oldDocument = updateDocument(document, updateQuery, matchPos);
                if (returnNew) {
                    returnDocument = document;
                } else {
                    returnDocument = oldDocument;
                }
                lastErrorObject = new Document("updatedExisting", Boolean.TRUE);
                lastErrorObject.put("n", Integer.valueOf(1));
            }
        }
        if (num == 0 && Utils.isTrue(query.get("upsert"))) {
            Document selector = (Document) query.get("query");
            Document updateQuery = (Document) query.get("update");
            Document newDocument = handleUpsert(updateQuery, selector);
            if (returnNew) {
                returnDocument = newDocument;
            } else {
                returnDocument = new Document();
            }
            num++;
        }

        if (query.get("fields") != null) {
            Document fields = (Document) query.get("fields");
            returnDocument = projectDocument(returnDocument, fields, idField);
        }

        Document result = new Document();
        if (lastErrorObject != null) {
            result.put("lastErrorObject", lastErrorObject);
        }
        result.put("value", returnDocument);
        Utils.markOkay(result);
        return result;
    }

    private static Document projectDocument(Document document, Document fields, String idField) {

        if (document == null) {
            return null;
        }

        Document newDocument = new Document();
        if (onlyExclusions(fields)) {
            newDocument.putAll(document);
            for (String excludedField : fields.keySet()) {
                newDocument.remove(excludedField);
            }
        } else {
            for (String key : fields.keySet()) {
                if (Utils.isTrue(fields.get(key))) {
                    projectField(document, newDocument, key);
                }
            }
        }

        // implicitly add _id if not mentioned
        // http://docs.mongodb.org/manual/tutorial/project-fields-from-query-results/#return-the-specified-fields-and-the-id-field-only
        if (!fields.containsKey(idField)) {
            newDocument.put(idField, document.get(idField));
        }

        return newDocument;
    }

    private static boolean onlyExclusions(Document fields) {
        for (String key : fields.keySet()) {
            if (Utils.isTrue(fields.get(key))) {
                return false;
            }
        }
        return true;
    }

    private static void projectField(Document document, Document newDocument, String key) {

        if (document == null) {
            return;
        }

        int dotPos = key.indexOf('.');
        if (dotPos > 0) {
            String mainKey = key.substring(0, dotPos);
            String subKey = key.substring(dotPos + 1);

            Object object = document.get(mainKey);
            // do not project the subdocument if it is not of type Document
            if (object instanceof Document) {
                if (!newDocument.containsKey(mainKey)) {
                    newDocument.put(mainKey, new Document());
                }
                projectField((Document) object, (Document) newDocument.get(mainKey), subKey);
            }
        } else {
            newDocument.put(key, document.get(key));
        }
    }

    private synchronized Iterable<Document> handleQuery(Document queryObject, int numberToSkip, int numberToReturn)
            throws MongoServerException {
        return handleQuery(queryObject, numberToSkip, numberToReturn, null);
    }

    @Override
    public synchronized Iterable<Document> handleQuery(Document queryObject, int numberToSkip, int numberToReturn,
            Document fieldSelector) throws MongoServerException {

        final Document query;
        final Document orderBy;

        if (numberToReturn < 0) {
            // actually: request to close cursor automatically
            numberToReturn = -numberToReturn;
        }

        if (queryObject.containsKey("query")) {
            query = (Document) queryObject.get("query");
            orderBy = (Document) queryObject.get("orderby");
        } else if (queryObject.containsKey("$query")) {
            query = (Document) queryObject.get("$query");
            orderBy = (Document) queryObject.get("$orderby");
        } else {
            query = queryObject;
            orderBy = null;
        }

        if (count() == 0) {
            return Collections.emptyList();
        }

        Iterable<Document> objs = queryDocuments(query, orderBy, numberToSkip, numberToReturn);

        if (fieldSelector != null && !fieldSelector.keySet().isEmpty()) {
            return new ProjectingIterable(objs, fieldSelector, idField);
        }

        return objs;
    }

    private static class ProjectingIterator implements Iterator<Document> {

        private Iterator<Document> iterator;
        private Document fieldSelector;
        private String idField;

        ProjectingIterator(Iterator<Document> iterator, Document fieldSelector, String idField) {
            this.iterator = iterator;
            this.fieldSelector = fieldSelector;
            this.idField = idField;
        }

        @Override
        public boolean hasNext() {
            return this.iterator.hasNext();
        }

        @Override
        public Document next() {
            Document document = this.iterator.next();
            return projectDocument(document, fieldSelector, idField);
        }

        @Override
        public void remove() {
            this.iterator.remove();
        }

    }

    private static class ProjectingIterable implements Iterable<Document> {

        private Iterable<Document> iterable;
        private Document fieldSelector;
        private String idField;

        ProjectingIterable(Iterable<Document> iterable, Document fieldSelector, String idField) {
            this.iterable = iterable;
            this.fieldSelector = fieldSelector;
            this.idField = idField;
        }

        @Override
        public Iterator<Document> iterator() {
            return new ProjectingIterator(iterable.iterator(), fieldSelector, idField);
        }
    }

    @Override
    public synchronized Document handleDistinct(Document query) throws MongoServerException {
        String key = query.get("key").toString();
        Document filter = (Document) query.get("query");
        if (filter == null) {
            filter = new Document();
        }
        Set<Object> values = new TreeSet<>(new ValueComparator());

        for (Document document : queryDocuments(filter, null, 0, 0)) {
            if (document.containsKey(key)) {
                values.add(document.get(key));
            }
        }

        Document response = new Document("values", new ArrayList<>(values));
        Utils.markOkay(response);
        return response;
    }

    @Override
    public synchronized int insertDocuments(List<Document> documents) throws MongoServerException {
        for (Document document : documents) {
            addDocument(document);
        }
        return documents.size();
    }

    @Override
    public synchronized int deleteDocuments(Document selector, int limit) throws MongoServerException {
        int n = 0;
        for (Document document : handleQuery(selector, 0, limit)) {
            if (limit > 0 && n >= limit) {
                throw new MongoServerException("internal error: too many elements (" + n + " >= " + limit + ")");
            }
            removeDocument(document);
            n++;
        }
        return n;
    }

    @Override
    public synchronized Document updateDocuments(Document selector, Document updateQuery, boolean isMulti,
            boolean isUpsert) throws MongoServerException {

        if (isMulti) {
            for (String key : updateQuery.keySet()) {
                if (!key.startsWith("$")) {
                    throw new MongoServerError(10158, "multi update only works with $ operators");
                }
            }
        }

        int nMatched = 0;
        int nModified = 0;
        for (Document document : queryDocuments(selector, null, 0, 0)) {
            Integer matchPos = matcher.matchPosition(document, selector);
            Document oldDocument = updateDocument(document, updateQuery, matchPos);
            if (!Utils.nullAwareEquals(oldDocument, document)) {
                nModified++;
            }
            nMatched++;

            if (!isMulti) {
                break;
            }
        }

        Document result = new Document();

        // insert?
        if (nModified == 0 && isUpsert) {
            Document newDocument = handleUpsert(updateQuery, selector);
            if (!selector.containsKey(idField)) {
                result.put("upserted", newDocument.get(idField));
            }
        }

        result.put("n", Integer.valueOf(nMatched));
        result.put("nModified", Integer.valueOf(nModified));
        return result;
    }

    private Document updateDocument(Document document, Document updateQuery, Integer matchPos)
            throws MongoServerException {
        synchronized (document) {
            // copy document
            Document oldDocument = new Document();
            cloneInto(oldDocument, document);

            Document newDocument = calculateUpdateDocument(document, updateQuery, matchPos, false);

            if (!newDocument.equals(oldDocument)) {
                for (Index<P> index : indexes) {
                    index.checkUpdate(oldDocument, newDocument);
                }
                for (Index<P> index : indexes) {
                    index.updateInPlace(oldDocument, newDocument);
                }

                long oldSize = Utils.calculateSize(oldDocument);
                long newSize = Utils.calculateSize(newDocument);
                updateDataSize(newSize - oldSize);

                // only keep fields that are also in the updated document
                Set<String> fields = new LinkedHashSet<>(document.keySet());
                fields.removeAll(newDocument.keySet());
                for (String key : fields) {
                    document.remove(key);
                }

                // update the fields
                for (String key : newDocument.keySet()) {
                    if (key.contains(".")) {
                        throw new MongoServerException(
                                "illegal field name. must not happen as it must be caught by the driver");
                    }
                    document.put(key, newDocument.get(key));
                }
                handleUpdate(document);
            }
            return oldDocument;
        }
    }

    protected abstract void handleUpdate(Document document) throws MongoServerException;

    private void cloneInto(Document targetDocument, Document sourceDocument) {
        for (String key : sourceDocument.keySet()) {
            targetDocument.put(key, cloneValue(sourceDocument.get(key)));
        }
    }

    private Object cloneValue(Object value) {
        if (value instanceof Document) {
            Document newValue = new Document();
            cloneInto(newValue, (Document) value);
            return newValue;
        } else if (value instanceof List<?>) {
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) value;
            List<Object> newValue = new ArrayList<>();
            for (Object v : list) {
                newValue.add(cloneValue(v));
            }
            return newValue;
        } else {
            return value;
        }
    }

    private Document handleUpsert(Document updateQuery, Document selector) throws MongoServerException {
        Document document = convertSelectorToDocument(selector);

        Document newDocument = calculateUpdateDocument(document, updateQuery, null, true);
        if (newDocument.get(idField) == null) {
            newDocument.put(idField, deriveDocumentId(selector));
        }
        addDocument(newDocument);
        return newDocument;
    }

    /**
     * convert selector used in an upsert statement into a document
     */
    Document convertSelectorToDocument(Document selector) throws MongoServerException {
        Document document = new Document();
        for (String key : selector.keySet()) {
            if (key.startsWith("$")) {
                continue;
            }

            Object value = selector.get(key);
            if (!Utils.containsQueryExpression(value)) {
                changeSubdocumentValue(document, key, value, (AtomicReference<Integer>) null);
            }
        }
        return document;
    }

    @Override
    public int getNumIndexes() {
        return indexes.size();
    }

    @Override
    public int count(Document query, int skip, int limit) throws MongoServerException {
        if (query.keySet().isEmpty()) {
            int count = count();
            if (skip > 0) {
                count = Math.max(0, count - skip);
            }
            if (limit > 0) {
                return Math.min(limit, count);
            }
            return count;
        }

        int numberToReturn = (limit >= 0) ? limit : 0;
        int count = 0;
        Iterator<?> it = queryDocuments(query, null, skip, numberToReturn).iterator();
        while (it.hasNext()) {
            it.next();
            count++;
        }
        return count;
    }

    @Override
    public Document getStats() throws MongoServerException {
        long dataSize = getDataSize();

        Document response = new Document("ns", getFullName());
        response.put("count", Integer.valueOf(count()));
        response.put("size", Long.valueOf(dataSize));

        double averageSize = 0;
        if (count() > 0) {
            averageSize = dataSize / (double) count();
        }
        response.put("avgObjSize", Double.valueOf(averageSize));
        response.put("storageSize", Integer.valueOf(0));
        response.put("numExtents", Integer.valueOf(0));
        response.put("nindexes", Integer.valueOf(indexes.size()));
        Document indexSizes = new Document();
        for (Index<P> index : indexes) {
            indexSizes.put(index.getName(), Long.valueOf(index.getDataSize()));
        }

        response.put("indexSize", indexSizes);
        Utils.markOkay(response);
        return response;
    }

    @Override
    public synchronized void removeDocument(Document document) throws MongoServerException {
        P position = null;

        if (!indexes.isEmpty()) {
            for (Index<P> index : indexes) {
                position = index.remove(document);
            }
        } else {
            position = findDocumentPosition(document);
        }
        if (position == null) {
            // not found
            return;
        }

        updateDataSize(-Utils.calculateSize(document));

        removeDocument(position);
    }

    @Override
    public Document validate() throws MongoServerException {
        Document response = new Document("ns", getFullName());
        response.put("extentCount", Integer.valueOf(0));
        response.put("datasize", Long.valueOf(getDataSize()));
        response.put("nrecords", Integer.valueOf(getRecordCount()));
        response.put("padding", Integer.valueOf(1));
        response.put("deletedCount", Integer.valueOf(getDeletedCount()));
        response.put("deletedSize", Integer.valueOf(0));

        response.put("nIndexes", Integer.valueOf(indexes.size()));
        Document keysPerIndex = new Document();
        for (Index<P> index : indexes) {
            keysPerIndex.put(index.getName(), Long.valueOf(index.getCount()));
        }

        response.put("keysPerIndex", keysPerIndex);
        response.put("valid", Boolean.TRUE);
        response.put("errors", Collections.emptyList());
        Utils.markOkay(response);
        return response;
    }

    @Override
    public void renameTo(String newDatabaseName, String newCollectionName) throws MongoServerException {
        this.databaseName = newDatabaseName;
        this.collectionName = newCollectionName;
    }

    protected abstract void removeDocument(P position) throws MongoServerException;

    protected abstract P findDocumentPosition(Document document) throws MongoServerException;

    protected abstract int getRecordCount();

    protected abstract int getDeletedCount();

    @SuppressWarnings("unchecked")
    private static List<Object> asList(Object value) {
        return (List<Object>) value;
    }


}
