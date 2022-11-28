package de.bwaldvogel.mongo.backend;

import static de.bwaldvogel.mongo.backend.Constants.ID_FIELD;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.ObjectId;
import de.bwaldvogel.mongo.exception.ConflictingUpdateOperatorsException;
import de.bwaldvogel.mongo.exception.FailedToParseException;
import de.bwaldvogel.mongo.exception.ImmutableFieldException;
import de.bwaldvogel.mongo.exception.IndexBuildFailedException;
import de.bwaldvogel.mongo.exception.IndexKeySpecsConflictException;
import de.bwaldvogel.mongo.exception.IndexNotFoundException;
import de.bwaldvogel.mongo.exception.InvalidIdFieldError;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.exception.MongoServerException;
import de.bwaldvogel.mongo.oplog.Oplog;

public abstract class AbstractMongoCollection<P> implements MongoCollection<P> {

    private static final Logger log = LoggerFactory.getLogger(AbstractMongoCollection.class);

    private final UUID uuid = UUID.randomUUID();

    private MongoDatabase database;
    private String collectionName;
    private final List<Index<P>> indexes = new ArrayList<>();
    private final QueryMatcher matcher = new DefaultQueryMatcher();
    protected final CollectionOptions options;
    protected final CursorRegistry cursorRegistry;

    protected AbstractMongoCollection(MongoDatabase database, String collectionName, CollectionOptions options,
                                      CursorRegistry cursorRegistry) {
        this.database = Objects.requireNonNull(database);
        this.collectionName = Objects.requireNonNull(collectionName);
        this.options = Objects.requireNonNull(options);
        this.cursorRegistry = cursorRegistry;
    }

    @Override
    public UUID getUuid() {
        return uuid;
    }

    protected boolean documentMatchesQuery(Document document, Document query) {
        return matcher.matches(document, query);
    }

    protected QueryResult queryDocuments(Document query, Document orderBy, int numberToSkip, int limit, int batchSize,
                                         Document fieldSelector) {
        for (Index<P> index : indexes) {
            if (index.canHandle(query)) {
                Iterable<P> positions = index.getPositions(query);
                return matchDocuments(query, positions, orderBy, numberToSkip, limit, batchSize, fieldSelector);
            }
        }

        return matchDocuments(query, orderBy, numberToSkip, limit, batchSize, fieldSelector);
    }

    protected abstract QueryResult matchDocuments(Document query, Document orderBy, int numberToSkip,
                                                  int numberToReturn, int batchSize, Document fieldSelector);

    protected QueryResult matchDocumentsFromStream(Stream<Document> documentStream, Document query, Document orderBy,
                                                   int numberToSkip, int limit, int batchSize, Document fieldSelector) {
        Comparator<Document> documentComparator = deriveComparator(orderBy);
        return matchDocumentsFromStream(query, documentStream, numberToSkip, limit, batchSize, documentComparator, fieldSelector);
    }

    protected QueryResult matchDocumentsFromStream(Document query, Stream<Document> documentStream,
                                                   int numberToSkip, int limit, int batchSize,
                                                   Comparator<Document> documentComparator,
                                                   Document fieldSelector) {
        documentStream = documentStream
            .filter(document -> documentMatchesQuery(document, query));

        if (documentComparator != null) {
            documentStream = documentStream.sorted(documentComparator);
        }

        if (numberToSkip > 0) {
            documentStream = documentStream.skip(numberToSkip);
        }

        if (limit > 0) {
            documentStream = documentStream.limit(limit);
        }

        if (fieldSelector != null && !fieldSelector.keySet().isEmpty()) {
            Projection projection = new Projection(fieldSelector, getIdField());
            documentStream = documentStream.map(projection::projectDocument);
        }

        List<Document> matchedDocuments = documentStream.collect(Collectors.toList());
        return createQueryResult(matchedDocuments, batchSize);
    }

    protected QueryResult matchDocuments(Document query, Iterable<P> positions, Document orderBy,
                                         int numberToSkip, int limit, int batchSize,
                                         Document fieldSelector) {
        Stream<Document> documentStream = StreamSupport.stream(positions.spliterator(), false)
            .map(this::getDocument);

        return matchDocumentsFromStream(documentStream, query, orderBy, numberToSkip, limit, batchSize, fieldSelector);
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
    public void addDocument(Document document) {
        if (document.get(ID_FIELD) instanceof Collection) {
            throw new InvalidIdFieldError("The '_id' value cannot be of type array");
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

        if (tracksDataSize()) {
            updateDataSize(Utils.calculateSize(document));
        }
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
                throw new IndexKeySpecsConflictException(index, existingIndex);
            }
            log.debug("Index with name '{}' already exists", index.getName());
            return;
        }
        if (index.isEmpty()) {
            try {
                streamAllDocumentsWithPosition().forEach(documentWithPosition -> {
                    Document document = documentWithPosition.getDocument();
                    index.checkAdd(document, this);
                });
                streamAllDocumentsWithPosition().forEach(documentWithPosition -> {
                    Document document = documentWithPosition.getDocument();
                    P position = documentWithPosition.getPosition();
                    index.add(document, position, this);
                });
            } catch (MongoServerError e) {
                throw new IndexBuildFailedException(e, this);
            }
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
            throw new IndexNotFoundException("index not found with name [" + indexName + "]");
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

        newDocument.cloneInto(oldDocument);
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
            oldDocument.cloneInto(newDocument);
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
    public Document findAndModify(Document query) {
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
        boolean matchingDocument = false;
        for (Document document : handleQuery(queryObject, 0, 1)) {
            matchingDocument = true;
            if (Utils.isTrue(query.get("remove"))) {
                removeDocument(document);
                returnDocument = document;
            } else if (query.get("update") != null) {
                Document updateQuery = (Document) query.get("update");

                Integer matchPos = matcher.matchPosition(document, (Document) queryObject.get("query"));

                ArrayFilters arrayFilters = ArrayFilters.parse(query, updateQuery);

                final Document oldDocument;
                try {
                    oldDocument = updateDocument(document, updateQuery, arrayFilters, matchPos);
                } catch (MongoServerError e) {
                    if (e.shouldPrefixCommandContext()) {
                        throw new FindAndModifyPlanExecutorError(e);

                    } else {
                        throw e;
                    }
                }

                if (returnNew) {
                    returnDocument = document;
                } else {
                    returnDocument = oldDocument;
                }
                lastErrorObject = new Document("updatedExisting", Boolean.TRUE);
                lastErrorObject.put("n", Integer.valueOf(1));
            }
        }

        if (!matchingDocument && Utils.isTrue(query.get("upsert"))) {
            Document selector = (Document) query.get("query");
            Document updateQuery = (Document) query.get("update");
            ArrayFilters arrayFilters = ArrayFilters.parse(query, updateQuery);
            Document newDocument = handleUpsert(updateQuery, selector, arrayFilters);
            if (returnNew) {
                returnDocument = newDocument;
            } else {
                returnDocument = null;
            }
        }

        Document fields = (Document) query.get("fields");
        if (fields != null) {
            returnDocument = new Projection(fields, getIdField()).projectDocument(returnDocument);
        }

        Document result = new Document();
        if (lastErrorObject != null) {
            result.put("lastErrorObject", lastErrorObject);
        }
        result.put("value", returnDocument);
        Utils.markOkay(result);
        return result;
    }

    public static class FindAndModifyPlanExecutorError extends MongoServerError {

        private static final long serialVersionUID = 1L;

        private static final String PREFIX = "Plan executor error during findAndModify :: caused by :: ";

        private FindAndModifyPlanExecutorError(MongoServerError cause) {
            super(cause.getCode(), cause.getCodeName(), PREFIX + cause.getMessageWithoutErrorCode(), cause);
        }

        @Override
        public synchronized MongoServerError getCause() {
            return (MongoServerError) super.getCause();
        }
    }

    @Override
    public QueryResult handleQuery(QueryParameters queryParameters) {
        final Document query;
        final Document orderBy;
        Document querySelector = queryParameters.getQuerySelector();
        if (querySelector.containsKey("query")) {
            query = (Document) querySelector.get("query");
            orderBy = (Document) querySelector.get("orderby");
        } else if (querySelector.containsKey("$query")) {
            query = (Document) querySelector.get("$query");
            orderBy = (Document) querySelector.get("$orderby");
        } else {
            query = querySelector;
            orderBy = null;
        }

        return queryDocuments(query, orderBy, queryParameters.getNumberToSkip(), queryParameters.getLimit(),
            queryParameters.getBatchSize(), queryParameters.getProjection());
    }

    @Override
    public List<Document> insertDocuments(List<Document> documents, boolean isOrdered) {
        int index = 0;
        List<Document> writeErrors = new ArrayList<>();
        for (Document document : documents) {
            try {
                addDocument(document);
            } catch (MongoServerError e) {
                writeErrors.add(toErrorDocument(e, index));
                if (isOrdered) {
                    break;
                }
            }
            index++;
        }
        return writeErrors;
    }

    private static Document toErrorDocument(MongoServerError e, int index) {
        Document error = new Document();
        error.put("index", index);
        error.put("errmsg", e.getMessageWithoutErrorCode());
        error.put("code", Integer.valueOf(e.getCode()));
        error.putIfNotNull("codeName", e.getCodeName());
        return error;
    }

    @Override
    public Document handleDistinct(Document query) {
        String key = (String) query.get("key");
        Document filter = (Document) query.getOrDefault("query", new Document());
        Set<Object> values = new TreeSet<>(ValueComparator.ascWithoutListHandling());

        for (Document document : queryDocuments(filter, null, 0, 0, 0, null)) {
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
    public int deleteDocuments(Document selector, int limit, Oplog oplog) {
        List<Object> deletedDocumentIds = new ArrayList<>();
        for (Document document : handleQuery(selector, 0, limit)) {
            if (limit > 0 && deletedDocumentIds.size() >= limit) {
                throw new MongoServerException("internal error: too many elements (" + deletedDocumentIds.size() + " >= " + limit + ")");
            }
            deletedDocumentIds.add(document.get(getIdField()));
            removeDocument(document);
        }
        oplog.handleDelete(getFullName(), selector, deletedDocumentIds);
        return deletedDocumentIds.size();
    }

    @Override
    public Document updateDocuments(Document selector, Document updateQuery, ArrayFilters arrayFilters,
                                    boolean isMulti, boolean isUpsert, Oplog oplog) {
        if (isMulti) {
            for (String key : updateQuery.keySet()) {
                if (!key.startsWith("$")) {
                    throw new MongoServerError(10158, "multi update only works with $ operators");
                }
            }
        }

        int nMatched = 0;
        List<Object> updatedIds = new ArrayList<>();
        for (Document document : queryDocuments(selector, null, 0, 0, 0, null)) {
            Integer matchPos = matcher.matchPosition(document, selector);
            Document oldDocument = updateDocument(document, updateQuery, arrayFilters, matchPos);
            if (!Utils.nullAwareEquals(oldDocument, document)) {
                updatedIds.add(document.get(getIdField()));
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
            oplog.handleInsert(getFullName(), Collections.singletonList(newDocument));
        } else {
            oplog.handleUpdate(getFullName(), selector, updateQuery, updatedIds);
        }

        result.put("n", Integer.valueOf(nMatched));
        result.put("nModified", Integer.valueOf(updatedIds.size()));
        return result;
    }

    private Document updateDocument(Document document, Document updateQuery,
                                    ArrayFilters arrayFilters, Integer matchPos) {
        Document oldDocument = document.cloneDeeply();

        Document newDocument = calculateUpdateDocument(document, updateQuery, arrayFilters, matchPos, false);

        if (!newDocument.equals(oldDocument)) {
            for (Index<P> index : indexes) {
                index.checkUpdate(oldDocument, newDocument, this);
            }
            P position = getSinglePosition(oldDocument);
            for (Index<P> index : indexes) {
                index.updateInPlace(oldDocument, newDocument, position, this);
            }

            if (tracksDataSize()) {
                int oldSize = Utils.calculateSize(oldDocument);
                int newSize = Utils.calculateSize(newDocument);
                updateDataSize(newSize - oldSize);
            }

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
        for (Map.Entry<String, Object> entry : selector.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            if (key.equals("$and")) {
                List<Document> andValues = (List<Document>) value;
                for (Document andValue : andValues) {
                    document.putAll(convertSelectorToDocument(andValue));
                }
                continue;
            } else if (key.equals("$or")) {
                List<Document> orValues = (List<Document>) value;
                if (orValues.size() == 1) {
                    document.putAll(convertSelectorToDocument(orValues.get(0)));
                }
                continue;
            } else if (key.startsWith("$")) {
                continue;
            }

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
        Iterator<?> it = queryDocuments(query, null, skip, numberToReturn, 0, new Document(getIdField(), 1)).iterator();
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
    public void removeDocument(Document document) {
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

        if (tracksDataSize()) {
            updateDataSize(-Utils.calculateSize(document));
        }

        removeDocument(position);
    }

    @VisibleForExternalBackends
    protected boolean tracksDataSize() {
        return true;
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

    protected QueryResult createQueryResult(List<Document> matchedDocuments, int batchSize) {
        final Collection<Document> firstBatch;
        if (batchSize > 0) {
            firstBatch = matchedDocuments.stream()
                .limit(batchSize)
                .collect(Collectors.toList());
        } else {
            firstBatch = matchedDocuments;
        }

        List<Document> remainingDocuments = matchedDocuments.subList(firstBatch.size(), matchedDocuments.size());

        if (remainingDocuments.isEmpty()) {
            return new QueryResult(firstBatch);
        } else {
            Cursor cursor = createCursor(remainingDocuments);
            return new QueryResult(firstBatch, cursor);
        }
    }

    protected Cursor createCursor(List<Document> remainingDocuments) {
        InMemoryCursor cursor = new InMemoryCursor(cursorRegistry.generateCursorId(), remainingDocuments);
        cursorRegistry.add(cursor);
        return cursor;
    }

}
