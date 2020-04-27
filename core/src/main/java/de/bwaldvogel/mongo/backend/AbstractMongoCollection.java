package de.bwaldvogel.mongo.backend;

import static de.bwaldvogel.mongo.backend.Constants.ID_FIELD;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.projection.ProjectingIterable;
import de.bwaldvogel.mongo.backend.projection.Projection;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.ObjectId;
import de.bwaldvogel.mongo.exception.BadValueException;
import de.bwaldvogel.mongo.exception.ConflictingUpdateOperatorsException;
import de.bwaldvogel.mongo.exception.CursorNotFoundException;
import de.bwaldvogel.mongo.exception.FailedToParseException;
import de.bwaldvogel.mongo.exception.ImmutableFieldException;
import de.bwaldvogel.mongo.exception.IndexOptionsConflictException;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.exception.MongoServerException;
import de.bwaldvogel.mongo.wire.message.MongoKillCursors;

public abstract class AbstractMongoCollection<P> implements MongoCollection<P> {

    private static final Logger log = LoggerFactory.getLogger(AbstractMongoCollection.class);

    private MongoDatabase database;
    private String collectionName;
    private final List<Index<P>> indexes = new ArrayList<>();
    private final QueryMatcher matcher = new DefaultQueryMatcher();
    protected final CollectionOptions options;
    protected final ConcurrentMap<Long, Cursor> cursors = new ConcurrentHashMap<>();
    private final AtomicLong cursorIdCounter = new AtomicLong();

    protected AbstractMongoCollection(MongoDatabase database, String collectionName, CollectionOptions options) {
        this.database = Objects.requireNonNull(database);
        this.collectionName = Objects.requireNonNull(collectionName);
        this.options = Objects.requireNonNull(options);
    }

    protected boolean documentMatchesQuery(Document document, Document query) {
        return matcher.matches(document, query);
    }

    private QueryResult queryDocuments(Document query, Document orderBy, int numberToSkip, int numberToReturn) {
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
        DocumentComparator documentComparator = deriveComparator(orderBy);
        if (documentComparator != null) {
            documents.sort(documentComparator);
        } else if (isNaturalDescending(orderBy)) {
            Collections.reverse(documents);
        }
    }

    protected abstract QueryResult matchDocuments(Document query, Document orderBy, int numberToSkip,
                                                  int numberToReturn);

    protected QueryResult matchDocuments(Document query, Iterable<P> positions, Document orderBy,
                                         int numberToSkip, int numberToReturn) {
        List<Document> matchedDocuments = new ArrayList<>();

        for (P position : positions) {
            Document document = getDocument(position);
            if (documentMatchesQuery(document, query)) {
                matchedDocuments.add(document);
            }
        }

        sortDocumentsInMemory(matchedDocuments, orderBy);

        if (numberToSkip > 0) {
            matchedDocuments = matchedDocuments.subList(numberToSkip, matchedDocuments.size());
        }

        if (numberToReturn > 0 && matchedDocuments.size() > numberToReturn) {
            matchedDocuments = matchedDocuments.subList(0, numberToReturn);
        }

        return new QueryResult(matchedDocuments);
    }

    protected static boolean isNaturalDescending(Document orderBy) {
        if (orderBy != null && !orderBy.keySet().isEmpty()) {
            if (orderBy.keySet().iterator().next().equals("$natural")) {
                Number sortValue = (Number) orderBy.get("$natural");
                if (sortValue.intValue() == -1) {
                    return true;
                }

                if (sortValue.intValue() != 1) {
                    throw new IllegalArgumentException("Illegal sort value: " + sortValue);
                }
            }
        }
        return false;
    }

    protected static DocumentComparator deriveComparator(Document orderBy) {
        if (orderBy != null && !orderBy.keySet().isEmpty()) {
            if (orderBy.keySet().iterator().next().equals("$natural")) {
                // already sorted
            } else {
                return new DocumentComparator(orderBy);
            }
        }
        return null;
    }

    protected abstract Document getDocument(P position);

    protected abstract void updateDataSize(int sizeDelta);

    protected abstract int getDataSize();

    protected abstract P addDocumentInternal(Document document);

    @Override
    public synchronized void addDocument(Document document) {
        if (document.get(ID_FIELD) instanceof Collection) {
            throw new BadValueException("can't use an array for _id");
        }

        if (!document.containsKey(ID_FIELD) && !isSystemCollection()) {
            ObjectId generatedObjectId = new ObjectId();
            log.debug("Generated {} for {} in {}", generatedObjectId, document, this);
            document.put(ID_FIELD, generatedObjectId);
        }

        for (Index<P> index : indexes) {
            index.checkAdd(document, this);
        }

        P position = addDocumentInternal(document);

        for (Index<P> index : indexes) {
            index.add(document, position, this);
        }

        updateDataSize(Utils.calculateSize(document));
    }

    @Override
    public MongoDatabase getDatabase() {
        return database;
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
        Index<P> existingIndex = findByName(index.getName());
        if (existingIndex != null) {
            if (!existingIndex.hasSameOptions(index)) {
                throw new IndexOptionsConflictException(existingIndex);
            }
            log.debug("Index with name '{}' already exists", index.getName());
            return;
        }
        if (index.isEmpty()) {
            streamAllDocumentsWithPosition().forEach(documentWithPosition -> {
                Document document = documentWithPosition.getDocument();
                index.checkAdd(document, this);
            });
            streamAllDocumentsWithPosition().forEach(documentWithPosition -> {
                Document document = documentWithPosition.getDocument();
                P position = documentWithPosition.getPosition();
                index.add(document, position, this);
            });
        } else {
            log.debug("Index is not empty");
        }
        indexes.add(index);
    }

    private Index<P> findByName(String indexName) {
        return indexes.stream()
            .filter(index -> index.getName().equals(indexName))
            .findFirst()
            .orElse(null);
    }

    @Override
    public void drop() {
        log.debug("Dropping collection {}", getFullName());
        Assert.isEmpty(indexes);
    }

    @Override
    public void dropIndex(String indexName) {
        log.debug("Dropping index '{}'", indexName);
        List<Index<P>> indexesToDrop = indexes.stream()
            .filter(index -> index.getName().equals(indexName))
            .collect(Collectors.toList());
        if (indexesToDrop.isEmpty()) {
            return;
        }
        Index<P> indexToDrop = CollectionUtils.getSingleElement(indexesToDrop);
        indexToDrop.drop();
        indexes.remove(indexToDrop);
    }

    private void modifyField(Document document, String modifier, Document update, ArrayFilters arrayFilters,
                             Integer matchPos, boolean isUpsert) {
        Document change = (Document) update.get(modifier);
        UpdateOperator updateOperator = getUpdateOperator(modifier, change);
        FieldUpdates updates = new FieldUpdates(document, updateOperator, getIdField(), isUpsert, matchPos, arrayFilters);
        updates.apply(change, modifier);
    }

    protected String getIdField() {
        return options.getIdField();
    }

    private UpdateOperator getUpdateOperator(String modifier, Document change) {
        final UpdateOperator op;
        try {
            op = UpdateOperator.fromValue(modifier);
        } catch (IllegalArgumentException e) {
            throw new FailedToParseException("Unknown modifier: " + modifier + ". Expected a valid update modifier or pipeline-style update specified as an array");
        }

        return op;
    }

    private void applyUpdate(Document oldDocument, Document newDocument) {
        if (newDocument.equals(oldDocument)) {
            return;
        }

        Object oldId = oldDocument.get(getIdField());
        Object newId = newDocument.get(getIdField());

        if (newId != null && oldId != null && !Utils.nullAwareEquals(oldId, newId)) {
            throw new ImmutableFieldException("After applying the update, the (immutable) field '_id' was found to have been altered to _id: " + newId);
        }

        if (newId == null && oldId != null) {
            newDocument.put(getIdField(), oldId);
        }

        cloneInto(oldDocument, newDocument);
    }

    Object deriveDocumentId(Document selector) {
        Object value = selector.get(getIdField());
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

    private Document calculateUpdateDocument(Document oldDocument, Document update, ArrayFilters arrayFilters,
                                             Integer matchPos, boolean isUpsert) {

        int numStartsWithDollar = 0;
        for (String key : update.keySet()) {
            if (key.startsWith("$")) {
                numStartsWithDollar++;
            }
        }

        Document newDocument = new Document();
        if (getIdField() != null) {
            newDocument.put(getIdField(), oldDocument.get(getIdField()));
        }

        if (numStartsWithDollar == update.keySet().size()) {
            validateUpdateQuery(update);
            cloneInto(newDocument, oldDocument);
            for (String key : update.keySet()) {
                modifyField(newDocument, key, update, arrayFilters, matchPos, isUpsert);
            }
        } else if (numStartsWithDollar == 0) {
            applyUpdate(newDocument, update);
        } else {
            throw new MongoServerException("illegal update: " + update);
        }

        Utils.validateFieldNames(newDocument);
        return newDocument;
    }

    static void validateUpdateQuery(Document update) {
        Set<String> allModifiedPaths = new LinkedHashSet<>();
        for (Object value : update.values()) {
            Document modification = (Document) value;
            for (String path : modification.keySet()) {
                for (String otherPath : allModifiedPaths) {
                    String commonPathPrefix = Utils.getShorterPathIfPrefix(path, otherPath);
                    if (commonPathPrefix != null) {
                        throw new ConflictingUpdateOperatorsException(path, commonPathPrefix);
                    }
                }
                allModifiedPaths.add(path);
            }
        }
    }

    @Override
    public synchronized Document findAndModify(Document query) {

        boolean returnNew = Utils.isTrue(query.get("new"));

        if (!query.containsKey("remove") && !query.containsKey("update")) {
            throw new FailedToParseException("Either an update or remove=true must be specified");
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

                ArrayFilters arrayFilters = ArrayFilters.parse(query, updateQuery);
                Document oldDocument = updateDocument(document, updateQuery, arrayFilters, matchPos);
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
            ArrayFilters arrayFilters = ArrayFilters.parse(query, updateQuery);
            Document newDocument = handleUpsert(updateQuery, selector, arrayFilters);
            if (returnNew) {
                returnDocument = newDocument;
            } else {
                returnDocument = new Document();
            }
            num++;
        }

        if (query.get("fields") != null) {
            Document fields = (Document) query.get("fields");
            returnDocument = Projection.projectDocument(returnDocument, fields, getIdField());
        }

        Document result = new Document();
        if (lastErrorObject != null) {
            result.put("lastErrorObject", lastErrorObject);
        }
        result.put("value", returnDocument);
        Utils.markOkay(result);
        return result;
    }

    @Override
    public synchronized QueryResult handleQuery(Document queryObject, int numberToSkip, int numberToReturn,
                                                Document fieldSelector) {

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

        QueryResult objs = queryDocuments(query, orderBy, numberToSkip, numberToReturn);

        if (fieldSelector != null && !fieldSelector.keySet().isEmpty()) {
            return new QueryResult(new ProjectingIterable(objs, fieldSelector, getIdField()), 0);
        }

        return objs;
    }

    @Override
    public synchronized QueryResult handleGetMore(long cursorId, int numberToReturn) {
        Cursor cursor = cursors.get(cursorId);
        if (cursor == null) {
            throw new CursorNotFoundException(String.format("Cursor id %d does not exists in collection %s", cursorId, collectionName));
        }
        List<Document> documents = cursor.takeDocuments(numberToReturn);

        if (cursor.isEmpty()) {
            log.debug("Removing empty {}", cursor);
            cursors.remove(cursor.getCursorId());
        }

        return new QueryResult(documents, cursor.isEmpty() ? EmptyCursor.get().getCursorId() : cursorId);
    }

    @Override
    public synchronized void handleKillCursors(MongoKillCursors killCursors) {
        killCursors.getCursorIds().forEach(cursors::remove);
    }

    @Override
    public synchronized Document handleDistinct(Document query) {
        String key = (String) query.get("key");
        Document filter = (Document) query.getOrDefault("query", new Document());
        Set<Object> values = new TreeSet<>(ValueComparator.ascWithoutListHandling().withDefaultComparatorForUuids());

        for (Document document : queryDocuments(filter, null, 0, 0)) {
            Object value = Utils.getSubdocumentValueCollectionAware(document, key);
            if (!(value instanceof Missing)) {
                if (value instanceof Collection) {
                    values.addAll((Collection<?>) value);
                } else {
                    values.add(value);
                }
            }
        }

        Document response = new Document("values", values);
        Utils.markOkay(response);
        return response;
    }

    @Override
    public synchronized void insertDocuments(List<Document> documents) {
        MongoCollection.super.insertDocuments(documents);
    }

    @Override
    public synchronized List<Document> deleteDocuments(Document selector, int limit) {
        List<Document> deleteDocuments = new ArrayList<>();
        for (Document document : handleQuery(selector, 0, limit)) {
            if (limit > 0 && deleteDocuments.size() >= limit) {
                throw new MongoServerException("internal error: too many elements (" + deleteDocuments.size() + " >= " + limit + ")");
            }
            deleteDocuments.add(document);
            removeDocument(document);
        }
        return deleteDocuments;
    }

    @Override
    public synchronized Document updateDocuments(Document selector, Document updateQuery, ArrayFilters arrayFilters,
                                                 boolean isMulti, boolean isUpsert) {

        if (isMulti) {
            for (String key : updateQuery.keySet()) {
                if (!key.startsWith("$")) {
                    throw new MongoServerError(10158, "multi update only works with $ operators");
                }
            }
        }

        int nMatched = 0;
        int nModified = 0;
        List<Object> updatedIds = new ArrayList<>();
        for (Document document : queryDocuments(selector, null, 0, 0)) {
            Integer matchPos = matcher.matchPosition(document, selector);
            Document oldDocument = updateDocument(document, updateQuery, arrayFilters, matchPos);
            if (!Utils.nullAwareEquals(oldDocument, document)) {
                updatedIds.add(document.get("_id"));
                nModified++;
            }
            nMatched++;

            if (!isMulti) {
                break;
            }
        }

        Document result = new Document();

        // insert?
        if (nMatched == 0 && isUpsert) {
            Document newDocument = handleUpsert(updateQuery, selector, arrayFilters);
            result.put("upserted", newDocument.get(getIdField()));
        }

        result.put("n", Integer.valueOf(nMatched));
        result.put("nModified", Integer.valueOf(nModified));
        result.put("modifiedIds", updatedIds);
        return result;
    }

    private Document updateDocument(Document document, Document updateQuery,
                                    ArrayFilters arrayFilters, Integer matchPos) {
        // copy document
        Document oldDocument = new Document();
        cloneInto(oldDocument, document);

        Document newDocument = calculateUpdateDocument(document, updateQuery, arrayFilters, matchPos, false);

        if (!newDocument.equals(oldDocument)) {
            for (Index<P> index : indexes) {
                index.checkUpdate(oldDocument, newDocument, this);
            }
            P position = getSinglePosition(oldDocument);
            for (Index<P> index : indexes) {
                index.updateInPlace(oldDocument, newDocument, position, this);
            }

            int oldSize = Utils.calculateSize(oldDocument);
            int newSize = Utils.calculateSize(newDocument);
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
            handleUpdate(position, oldDocument, document);
        }
        return oldDocument;
    }

    private P getSinglePosition(Document document) {
        if (indexes.isEmpty()) {
            return findDocumentPosition(document);
        }
        Set<P> positions = indexes.stream()
            .map(index -> index.getPosition(document))
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());
        return CollectionUtils.getSingleElement(positions);
    }

    protected abstract void handleUpdate(P position, Document oldDocument, Document newDocument);

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

    private Document handleUpsert(Document updateQuery, Document selector, ArrayFilters arrayFilters) {
        Document document = convertSelectorToDocument(selector);

        Document newDocument = calculateUpdateDocument(document, updateQuery, arrayFilters, null, true);
        newDocument.computeIfAbsent(getIdField(), k -> deriveDocumentId(selector));
        addDocument(newDocument);
        return newDocument;
    }

    /**
     * convert selector used in an upsert statement into a document
     */
    Document convertSelectorToDocument(Document selector) {
        Document document = new Document();
        for (String key : selector.keySet()) {
            if (key.startsWith("$")) {
                continue;
            }

            Object value = selector.get(key);
            if (!Utils.containsQueryExpression(value)) {
                Utils.changeSubdocumentValue(document, key, value, (AtomicReference<Integer>) null);
            }
        }
        return document;
    }

    @Override
    public List<Index<P>> getIndexes() {
        return indexes;
    }

    @Override
    public int count(Document query, int skip, int limit) {
        if (query == null || query.keySet().isEmpty()) {
            int count = count();
            if (skip > 0) {
                count = Math.max(0, count - skip);
            }
            if (limit > 0) {
                return Math.min(limit, count);
            }
            return count;
        }

        int numberToReturn = Math.max(limit, 0);
        int count = 0;
        Iterator<?> it = queryDocuments(query, null, skip, numberToReturn).iterator();
        while (it.hasNext()) {
            it.next();
            count++;
        }
        return count;
    }

    @Override
    public Document getStats() {
        int dataSize = getDataSize();
        int count = count();

        Document response = new Document("ns", getFullName());
        response.put("count", Integer.valueOf(count));
        response.put("size", Integer.valueOf(dataSize));

        int averageSize = 0;
        if (count > 0) {
            averageSize = dataSize / count;
        }
        response.put("avgObjSize", Integer.valueOf(averageSize));
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
    public synchronized void removeDocument(Document document) {
        P position = null;

        if (!indexes.isEmpty()) {
            for (Index<P> index : indexes) {
                P indexPosition = index.remove(document);
                if (indexPosition == null) {
                    if (index.isSparse()) {
                        continue;
                    } else {
                        throw new IllegalStateException("Found no position for " + document + " in " + index);
                    }
                }
                if (position != null) {
                    Assert.equals(position, indexPosition, () -> "Got different positions for " + document);
                }
                position = indexPosition;
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
    public Document validate() {
        Document response = new Document("ns", getFullName());
        response.put("extentCount", Integer.valueOf(0));
        response.put("datasize", Long.valueOf(getDataSize()));
        response.put("nrecords", Integer.valueOf(count()));

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
    public void renameTo(MongoDatabase newDatabase, String newCollectionName) {
        this.database = newDatabase;
        this.collectionName = newCollectionName;
    }

    protected abstract void removeDocument(P position);

    protected static Iterable<Document> applySkipAndLimit(List<Document> documents, int numberToSkip, int numberToReturn) {
        if (numberToSkip > 0) {
            if (numberToSkip < documents.size()) {
                documents = documents.subList(numberToSkip, documents.size());
            } else {
                return Collections.emptyList();
            }
        }

        if (numberToReturn > 0 && documents.size() > numberToReturn) {
            documents = documents.subList(0, numberToReturn);
        }

        return documents;
    }

    protected P findDocumentPosition(Document document) {
        return streamAllDocumentsWithPosition()
            .filter(match -> documentMatchesQuery(match.getDocument(), document))
            .map(DocumentWithPosition::getPosition)
            .findFirst()
            .orElse(null);
    }

    protected abstract Stream<DocumentWithPosition<P>> streamAllDocumentsWithPosition();

    private boolean isSystemCollection() {
        return AbstractMongoDatabase.isSystemCollection(getCollectionName());
    }

    protected QueryResult createQueryResult(List<Document> matchedDocuments, int numberToSkip, int numberToReturn) {
        Cursor cursor = createCursor(matchedDocuments, numberToSkip, numberToReturn);
        Iterable<Document> documents = applySkipAndLimit(matchedDocuments, numberToSkip, numberToReturn);
        return new QueryResult(documents, cursor.getCursorId());
    }

    protected Cursor createCursor(Collection<Document> matchedDocuments, int numberToSkip, int numberToReturn) {
        final List<Document> remainingDocuments;
        if (numberToReturn > 0 && matchedDocuments.size() > numberToReturn) {
            remainingDocuments = matchedDocuments.stream()
                .skip(numberToSkip + numberToReturn)
                .collect(Collectors.toList());
        } else {
            remainingDocuments = Collections.emptyList();
        }

        if (remainingDocuments.isEmpty()) {
            return EmptyCursor.get();
        }

        Cursor cursor = new InMemoryCursor(cursorIdCounter.incrementAndGet(), remainingDocuments);
        Cursor previousValue = cursors.put(cursor.getCursorId(), cursor);
        Assert.isNull(previousValue);
        return cursor;
    }

}
