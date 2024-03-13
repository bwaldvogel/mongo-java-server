package de.bwaldvogel.mongo.backend;

import static de.bwaldvogel.mongo.backend.Constants.ID_FIELD;
import static de.bwaldvogel.mongo.backend.Constants.PRIMARY_KEY_INDEX_NAME;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.aggregation.Aggregation;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.IndexNotFoundException;
import de.bwaldvogel.mongo.exception.InvalidNamespaceError;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.exception.MongoServerException;
import de.bwaldvogel.mongo.exception.MongoSilentServerException;
import de.bwaldvogel.mongo.exception.NamespaceExistsException;
import de.bwaldvogel.mongo.exception.NoSuchCommandException;
import de.bwaldvogel.mongo.oplog.NoopOplog;
import de.bwaldvogel.mongo.oplog.Oplog;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import io.netty.channel.Channel;

public abstract class AbstractMongoDatabase<P> implements MongoDatabase {

    private static final String NAMESPACES_COLLECTION_NAME = "system.namespaces";

    private static final String INDEXES_COLLECTION_NAME = "system.indexes";

    private static final Logger log = LoggerFactory.getLogger(AbstractMongoDatabase.class);

    protected final String databaseName;

    private final Map<String, MongoCollection<P>> collections = new ConcurrentHashMap<>();

    protected final AtomicReference<MongoCollection<P>> indexes = new AtomicReference<>();

    private final Map<Channel, List<Document>> lastResults = new ConcurrentHashMap<>();

    private MongoCollection<P> namespaces;

    protected final CursorRegistry cursorRegistry;

    protected AbstractMongoDatabase(String databaseName, CursorRegistry cursorRegistry) {
        this.databaseName = databaseName;
        this.cursorRegistry = cursorRegistry;
    }

    protected void initializeNamespacesAndIndexes() {
        this.namespaces = openOrCreateCollection(NAMESPACES_COLLECTION_NAME, CollectionOptions.withIdField("name"));
        this.collections.put(namespaces.getCollectionName(), namespaces);

        if (!namespaces.isEmpty()) {
            for (String name : listCollectionNamespaces()) {
                log.debug("opening {}", name);
                String collectionName = extractCollectionNameFromNamespace(name);
                MongoCollection<P> collection = openOrCreateCollection(collectionName, CollectionOptions.withDefaults());
                collections.put(collectionName, collection);
                log.debug("opened collection '{}'", collectionName);
            }

            MongoCollection<P> indexCollection = openOrCreateCollection(INDEXES_COLLECTION_NAME, CollectionOptions.withoutIdField());
            collections.put(indexCollection.getCollectionName(), indexCollection);
            this.indexes.set(indexCollection);
            for (Document indexDescription : indexCollection.queryAll()) {
                openOrCreateIndex(indexDescription);
            }
        }
    }

    @Override
    public final String getDatabaseName() {
        return databaseName;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" + getDatabaseName() + ")";
    }

    private Document commandError(Channel channel, String command, Document query) {
        // getlasterror must not clear the last error
        if (command.equalsIgnoreCase("getlasterror")) {
            return commandGetLastError(channel, command, query);
        } else if (command.equalsIgnoreCase("reseterror")) {
            return commandResetError(channel);
        }
        return null;
    }

    @Override
    public Document handleCommand(Channel channel, String command, Document query, DatabaseResolver databaseResolver, Oplog oplog) {
        Document commandErrorDocument = commandError(channel, command, query);
        if (commandErrorDocument != null) {
            return commandErrorDocument;
        }

        clearLastStatus(channel);

        if (command.equalsIgnoreCase("find")) {
            return commandFind(command, query);
        } else if (command.equalsIgnoreCase("insert")) {
            return commandInsert(channel, command, query, oplog);
        } else if (command.equalsIgnoreCase("update")) {
            return commandUpdate(channel, command, query, oplog);
        } else if (command.equalsIgnoreCase("delete")) {
            return commandDelete(channel, command, query, oplog);
        } else if (command.equalsIgnoreCase("create")) {
            return commandCreate(command, query);
        } else if (command.equalsIgnoreCase("createIndexes")) {
            String collectionName = (String) query.get(command);
            return commandCreateIndexes(query, collectionName);
        } else if (command.equalsIgnoreCase("count")) {
            return commandCount(command, query);
        } else if (command.equalsIgnoreCase("aggregate")) {
            return commandAggregate(command, query, databaseResolver, oplog);
        } else if (command.equalsIgnoreCase("distinct")) {
            MongoCollection<P> collection = resolveCollection(command, query);
            if (collection == null) {
                Document response = new Document("values", Collections.emptyList());
                Utils.markOkay(response);
                return response;
            } else {
                return collection.handleDistinct(query);
            }
        } else if (command.equalsIgnoreCase("drop")) {
            return commandDrop(query, oplog);
        } else if (command.equalsIgnoreCase("dropIndexes")) {
            return commandDropIndexes(query);
        } else if (command.equalsIgnoreCase("dbstats")) {
            return commandDatabaseStats();
        } else if (command.equalsIgnoreCase("collstats")) {
            MongoCollection<P> collection = resolveCollection(command, query);
            if (collection == null) {
                Document emptyStats = new Document()
                    .append("count", 0)
                    .append("size", 0);
                Utils.markOkay(emptyStats);
                return emptyStats;
            } else {
                return collection.getStats();
            }
        } else if (command.equalsIgnoreCase("validate")) {
            MongoCollection<P> collection = resolveCollection(command, query);
            if (collection == null) {
                String collectionName = query.get(command).toString();
                String fullCollectionName = getDatabaseName() + "." + collectionName;
                throw new MongoServerError(26, "NamespaceNotFound", "Collection '" + fullCollectionName + "' does not exist to validate.");
            }
            return collection.validate();
        } else if (command.equalsIgnoreCase("findAndModify")) {
            String collectionName = query.get(command).toString();
            MongoCollection<P> collection = resolveOrCreateCollection(collectionName);
            return collection.findAndModify(query);
        } else if (command.equalsIgnoreCase("listCollections")) {
            return listCollections();
        } else if (command.equalsIgnoreCase("listIndexes")) {
            String collectionName = query.get(command).toString();
            return listIndexes(collectionName);
        } else if (command.equals("triggerInternalException")) {
            throw new NullPointerException("For testing purposes");
        } else {
            log.error("unknown query: {}", query);
        }
        throw new NoSuchCommandException(command);
    }

    private Document listCollections() {
        List<Document> firstBatch = new ArrayList<>();
        for (String namespace : listCollectionNamespaces()) {
            if (namespace.endsWith(INDEXES_COLLECTION_NAME)) {
                continue;
            }
            Document collectionDescription = new Document();
            Document collectionOptions = new Document();
            String collectionName = extractCollectionNameFromNamespace(namespace);
            collectionDescription.put("name", collectionName);
            collectionDescription.put("options", collectionOptions);
            collectionDescription.put("info", new Document("readOnly", false));
            collectionDescription.put("type", "collection");
            collectionDescription.put("idIndex", getPrimaryKeyIndexDescription());
            firstBatch.add(collectionDescription);
        }

        return Utils.firstBatchCursorResponse(getDatabaseName() + ".$cmd.listCollections", firstBatch);
    }

    private static Document getPrimaryKeyIndexDescription() {
        return getPrimaryKeyIndexDescription(null);
    }

    private static Document getPrimaryKeyIndexDescription(String namespace) {
        Document indexDescription = new Document("key", new Document(ID_FIELD, 1))
            .append("name", PRIMARY_KEY_INDEX_NAME);

        if (namespace != null) {
            indexDescription.put("ns", namespace);
        }

        return indexDescription.append("v", 2);
    }

    private Iterable<String> listCollectionNamespaces() {
        return namespaces.queryAllAsStream()
            .map(document -> document.get("name").toString())
            ::iterator;
    }

    private Document listIndexes(String collectionName) {
        Stream<Document> indexes = Optional.ofNullable(resolveCollection(INDEXES_COLLECTION_NAME, false))
            .map(collection -> collection.handleQueryAsStream(new Document("ns", getFullCollectionNamespace(collectionName))))
            .orElse(Stream.empty())
            .map(indexDescription -> {
                Document clone = indexDescription.clone();
                clone.remove("ns");
                return clone;
            });
        return Utils.firstBatchCursorResponse(getDatabaseName() + ".$cmd.listIndexes", indexes);
    }

    protected MongoCollection<P> resolveOrCreateCollection(String collectionName) {
        final MongoCollection<P> collection = resolveCollection(collectionName, false);
        if (collection != null) {
            return collection;
        } else {
            return createCollection(collectionName, CollectionOptions.withDefaults());
        }
    }

    private Document commandFind(String command, Document query) {
        String collectionName = (String) query.get(command);
        MongoCollection<P> collection = resolveCollection(collectionName, false);
        if (collection == null) {
            return Utils.firstBatchCursorResponse(getFullCollectionNamespace(collectionName), Collections.emptyList());
        }
        QueryParameters queryParameters = toQueryParameters(query);
        QueryResult queryResult = collection.handleQuery(queryParameters);
        return toCursorResponse(collection, queryResult);
    }

    private static QueryParameters toQueryParameters(Document query) {
        int numberToSkip = ((Number) query.getOrDefault("skip", 0)).intValue();
        int numberToReturn = ((Number) query.getOrDefault("limit", 0)).intValue();
        int batchSize = ((Number) query.getOrDefault("batchSize", 0)).intValue();

        Document querySelector = new Document();
        querySelector.put("$query", query.getOrDefault("filter", new Document()));
        querySelector.put("$orderby", query.get("sort"));

        Document projection = (Document) query.get("projection");
        return new QueryParameters(querySelector, numberToSkip, numberToReturn, batchSize, projection);
    }

    private QueryParameters toQueryParameters(MongoQuery query, int numberToSkip, int batchSize) {
        return new QueryParameters(query.getQuery(), numberToSkip, 0, batchSize, query.getReturnFieldSelector());
    }

    private Document toCursorResponse(MongoCollection<P> collection, QueryResult queryResult) {
        List<Document> documents = new ArrayList<>();
        for (Document document : queryResult) {
            documents.add(document);
        }
        return Utils.firstBatchCursorResponse(collection.getFullName(), documents, queryResult.getCursorId());
    }

    private Document commandInsert(Channel channel, String command, Document query, Oplog oplog) {
        String collectionName = query.get(command).toString();
        boolean isOrdered = Utils.isTrue(query.get("ordered"));
        log.trace("ordered: {}", isOrdered);

        @SuppressWarnings("unchecked")
        List<Document> documents = (List<Document>) query.get("documents");

        List<Document> writeErrors = insertDocuments(channel, collectionName, documents, oplog, isOrdered);
        Document result = new Document();
        result.put("n", Integer.valueOf(documents.size()));
        if (!writeErrors.isEmpty()) {
            result.put("writeErrors", writeErrors);
        }
        // odd by true: also mark error as okay
        Utils.markOkay(result);
        return result;
    }

    private Document commandUpdate(Channel channel, String command, Document query, Oplog oplog) {
        clearLastStatus(channel);
        String collectionName = query.get(command).toString();
        boolean isOrdered = Utils.isTrue(query.get("ordered"));
        log.trace("ordered: {}", isOrdered);

        @SuppressWarnings("unchecked")
        List<Document> updates = (List<Document>) query.get("updates");
        int nMatched = 0;
        int nModified = 0;
        Collection<Document> upserts = new ArrayList<>();

        List<Document> writeErrors = new ArrayList<>();

        Document response = new Document();
        for (int i = 0; i < updates.size(); i++) {
            Document updateObj = updates.get(i);
            Document selector = (Document) updateObj.get("q");
            Document update = (Document) updateObj.get("u");
            ArrayFilters arrayFilters = ArrayFilters.parse(updateObj, update);
            boolean multi = Utils.isTrue(updateObj.get("multi"));
            boolean upsert = Utils.isTrue(updateObj.get("upsert"));
            final Document result;
            try {
                result = updateDocuments(collectionName, selector, update, arrayFilters, multi, upsert, oplog);
            } catch (MongoServerException e) {
                writeErrors.add(toWriteError(i, e));
                continue;
            }
            if (result.containsKey("upserted")) {
                final Object id = result.get("upserted");
                final Document upserted = new Document("index", i);
                upserted.put(ID_FIELD, id);
                upserts.add(upserted);
            }
            nMatched += ((Integer) result.get("n")).intValue();
            nModified += ((Integer) result.get("nModified")).intValue();
        }

        response.put("n", nMatched + upserts.size());
        response.put("nModified", nModified);
        if (!upserts.isEmpty()) {
            response.put("upserted", upserts);
        }
        if (!writeErrors.isEmpty()) {
            response.put("writeErrors", writeErrors);
        }
        Utils.markOkay(response);
        putLastResult(channel, response);
        return response;
    }

    private Document commandDelete(Channel channel, String command, Document query, Oplog oplog) {
        String collectionName = query.get(command).toString();
        boolean isOrdered = Utils.isTrue(query.get("ordered"));
        log.trace("ordered: {}", isOrdered);

        @SuppressWarnings("unchecked")
        List<Document> deletes = (List<Document>) query.get("deletes");
        int n = 0;
        for (Document delete : deletes) {
            final Document selector = (Document) delete.get("q");
            final int limit = ((Number) delete.get("limit")).intValue();
            Document result = deleteDocuments(channel, collectionName, selector, limit, oplog);
            Integer resultNumber = (Integer) result.get("n");
            n += resultNumber.intValue();
        }

        Document response = new Document("n", Integer.valueOf(n));
        Utils.markOkay(response);
        return response;
    }

    private Document commandCreate(String command, Document query) {
        String collectionName = query.get(command).toString();

        CollectionOptions collectionOptions = CollectionOptions.fromQuery(query);
        collectionOptions.validate();
        createCollectionOrThrowIfExists(collectionName, collectionOptions);

        Document response = new Document();
        Utils.markOkay(response);
        return response;
    }

    @Override
    public MongoCollection<P> createCollectionOrThrowIfExists(String collectionName, CollectionOptions options) {
        MongoCollection<P> collection = resolveCollection(collectionName, false);
        if (collection != null) {
            throw new NamespaceExistsException("Collection already exists. NS: " + collection.getFullName());
        }

        return createCollection(collectionName, options);
    }

    private Document commandCreateIndexes(Document query, String collectionName) {
        int indexesBefore = countIndexes();

        @SuppressWarnings("unchecked")
        Collection<Document> indexDescriptions = (Collection<Document>) query.get("indexes");
        for (Document indexDescription : indexDescriptions) {
            indexDescription.putIfAbsent("ns", getFullCollectionNamespace(collectionName));
            addIndex(indexDescription);
        }

        int indexesAfter = countIndexes();

        Document response = new Document();
        response.put("numIndexesBefore", Integer.valueOf(indexesBefore));
        response.put("numIndexesAfter", Integer.valueOf(indexesAfter));
        Utils.markOkay(response);
        return response;
    }

    private Document commandDropIndexes(Document query) {
        String collectionName = (String) query.get("dropIndexes");
        MongoCollection<P> collection = resolveCollection(collectionName, false);
        if (collection != null) {
            dropIndexes(collection, query);
        }
        Document response = new Document();
        Utils.markOkay(response);
        return response;
    }

    private void dropIndexes(MongoCollection<P> collection, Document query) {
        Object index = query.get("index");
        Assert.notNull(index, () -> "Index name must not be null");
        MongoCollection<P> indexCollection = indexes.get();
        if (Objects.equals(index, "*")) {
            for (Document indexDocument : indexCollection.queryAll()) {
                Document indexKeys = (Document) indexDocument.get("key");
                if (!isPrimaryKeyIndex(indexKeys)) {
                    dropIndex(collection, indexDocument);
                }
            }
        } else if (index instanceof String) {
            dropIndex(collection, new Document("name", index));
        } else {
            Document indexKeys = (Document) index;
            Document indexQuery = new Document("key", indexKeys).append("ns", collection.getFullName());
            Document indexToDrop = CollectionUtils.getSingleElement(indexCollection.handleQuery(indexQuery),
                () -> new IndexNotFoundException(indexKeys));
            int numDeleted = dropIndex(collection, indexToDrop);
            Assert.equals(numDeleted, 1, () -> "Expected one deleted document");
        }
    }

    private int dropIndex(MongoCollection<P> collection, Document indexDescription) {
        String indexName = (String) indexDescription.get("name");
        dropIndex(collection, indexName);
        return indexes.get().deleteDocuments(indexDescription, -1);
    }

    protected void dropIndex(MongoCollection<P> collection, String indexName) {
        collection.dropIndex(indexName);
    }

    protected int countIndexes() {
        MongoCollection<P> indexesCollection = indexes.get();
        if (indexesCollection == null) {
            return 0;
        } else {
            return indexesCollection.count();
        }
    }

    private Collection<MongoCollection<P>> collections() {
        return collections.values().stream()
            .filter(collection -> !isSystemCollection(collection.getCollectionName()))
            .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private Document commandDatabaseStats() {
        Document response = new Document("db", getDatabaseName());
        response.put("collections", Integer.valueOf(collections().size()));

        long storageSize = getStorageSize();
        long fileSize = getFileSize();
        long indexSize = 0;
        int objects = 0;
        double dataSize = 0;
        double averageObjectSize = 0;

        for (MongoCollection<P> collection : collections()) {
            Document stats = collection.getStats();
            objects += ((Number) stats.get("count")).intValue();
            dataSize += ((Number) stats.get("size")).doubleValue();

            Document indexSizes = (Document) stats.get("indexSize");
            for (String indexName : indexSizes.keySet()) {
                indexSize += ((Number) indexSizes.get(indexName)).longValue();
            }

        }
        if (objects > 0) {
            averageObjectSize = dataSize / ((double) objects);
        }
        response.put("objects", Integer.valueOf(objects));
        response.put("avgObjSize", Double.valueOf(averageObjectSize));
        if (dataSize == 0.0) {
            response.put("dataSize", Integer.valueOf(0));
        } else {
            response.put("dataSize", Double.valueOf(dataSize));
        }
        response.put("storageSize", Long.valueOf(storageSize));
        response.put("numExtents", Integer.valueOf(0));
        response.put("indexes", Integer.valueOf(countIndexes()));
        response.put("indexSize", Long.valueOf(indexSize));
        response.put("fileSize", Long.valueOf(fileSize));
        response.put("nsSizeMB", Integer.valueOf(0));
        Utils.markOkay(response);
        return response;
    }

    protected abstract long getFileSize();

    protected abstract long getStorageSize();

    private Document commandDrop(Document query, Oplog oplog) {
        String collectionName = query.get("drop").toString();

        MongoCollection<P> collection = resolveCollection(collectionName, false);
        if (collection == null) {
            throw new MongoSilentServerException("ns not found");
        }

        int numIndexes = collection.getNumIndexes();
        dropCollection(collectionName, oplog);
        Document response = new Document();
        response.put("nIndexesWas", Integer.valueOf(numIndexes));
        response.put("ns", collection.getFullName());
        Utils.markOkay(response);
        return response;
    }

    private Document commandGetLastError(Channel channel, String command, Document query) {
        query.forEach((subCommand, value) -> {
            if (subCommand.equals(command)) {
                return;
            }

            switch (subCommand) {
                case "w":
                    // ignore
                    break;
                case "fsync":
                    // ignore
                    break;
                case "$db":
                    Assert.equals(value, getDatabaseName());
                    break;
                default:
                    throw new MongoServerException("unknown subcommand: " + subCommand);
            }
        });

        List<Document> results = lastResults.get(channel);

        Document result;
        if (results != null && !results.isEmpty()) {
            result = results.get(results.size() - 1);
            if (result == null) {
                result = new Document();
            }
        } else {
            result = new Document();
            result.put("err", null);
            result.put("n", 0);
        }
        if (result.containsKey("writeErrors")) {
            @SuppressWarnings("unchecked")
            List<Document> writeErrors = (List<Document>) result.get("writeErrors");
            if (writeErrors.size() == 1) {
                result.putAll(CollectionUtils.getSingleElement(writeErrors));
                result.remove("writeErrors");
            }
        }
        Utils.markOkay(result);
        return result;
    }

    private Document commandResetError(Channel channel) {
        List<Document> results = lastResults.get(channel);
        if (results != null) {
            results.clear();
        }
        Document result = new Document();
        Utils.markOkay(result);
        return result;
    }

    private Document commandCount(String command, Document query) {
        MongoCollection<P> collection = resolveCollection(command, query);
        Document response = new Document();
        if (collection == null) {
            response.put("n", Integer.valueOf(0));
        } else {
            Document queryObject = (Document) query.get("query");
            int limit = getOptionalNumber(query, "limit", -1);
            int skip = getOptionalNumber(query, "skip", 0);
            response.put("n", Integer.valueOf(collection.count(queryObject, skip, limit)));
        }
        Utils.markOkay(response);
        return response;
    }

    private Document commandAggregate(String command, Document query, DatabaseResolver databaseResolver, Oplog oplog) {
        String collectionName = query.get(command).toString();
        MongoCollection<P> collection = resolveCollection(collectionName, false);
        Object pipelineObject = Aggregation.parse(query.get("pipeline"));
        List<Document> pipeline = Aggregation.parse(pipelineObject);
        if (!pipeline.isEmpty()) {
            Document changeStream = (Document) pipeline.get(0).get("$changeStream");
            if (changeStream != null) {
                Aggregation aggregation = getAggregation(pipeline.subList(1, pipeline.size()), query, databaseResolver, collection, oplog);
                aggregation.validate(query);
                return commandChangeStreamPipeline(query, oplog, collectionName, changeStream, aggregation);
            }
        }

        Aggregation aggregation = getAggregation(pipeline, query, databaseResolver, collection, oplog);
        return Utils.firstBatchCursorResponse(getFullCollectionNamespace(collectionName), aggregation.computeResult());
    }

    private Aggregation getAggregation(List<Document> pipeline, Document query, DatabaseResolver databaseResolver,
                                       MongoCollection<?> collection, Oplog oplog) {
        Aggregation aggregation = Aggregation.fromPipeline(pipeline, databaseResolver, this, collection, oplog);
        aggregation.validate(query);
        return aggregation;
    }

    private Document commandChangeStreamPipeline(Document query, Oplog oplog, String collectionName, Document changeStreamDocument,
                                                 Aggregation aggregation) {
        Document cursorDocument = (Document) query.get("cursor");
        int batchSize = (int) cursorDocument.getOrDefault("batchSize", 0);

        String namespace = getFullCollectionNamespace(collectionName);
        Cursor cursor = oplog.createCursor(changeStreamDocument, namespace, aggregation);
        return Utils.firstBatchCursorResponse(namespace, cursor.takeDocuments(batchSize), cursor);
    }

    private int getOptionalNumber(Document query, String fieldName, int defaultValue) {
        Number limitNumber = (Number) query.get(fieldName);
        return limitNumber != null ? limitNumber.intValue() : defaultValue;
    }

    @Override
    public QueryResult handleQuery(MongoQuery query) {
        clearLastStatus(query.getChannel());
        String collectionName = query.getCollectionName();
        MongoCollection<P> collection = resolveCollection(collectionName, false);
        if (collection == null) {
            return new QueryResult();
        }
        int numberToSkip = query.getNumberToSkip();
        int batchSize = query.getNumberToReturn();

        if (batchSize < -1) {
            // actually: request to close cursor automatically
            batchSize = -batchSize;
        }

        QueryParameters queryParameters = toQueryParameters(query, numberToSkip, batchSize);
        return collection.handleQuery(queryParameters);
    }

    @Override
    public void handleClose(Channel channel) {
        lastResults.remove(channel);
    }

    protected void clearLastStatus(Channel channel) {
        List<Document> results = lastResults.computeIfAbsent(channel, k -> new LimitedList<>(10));
        results.add(null);
    }

    private MongoCollection<P> resolveCollection(String command, Document query) {
        String collectionName = query.get(command).toString();
        return resolveCollection(collectionName, false);
    }

    @Override
    public MongoCollection<P> resolveCollection(String collectionName, boolean throwIfNotFound) {
        checkCollectionName(collectionName);
        MongoCollection<P> collection = collections.get(collectionName);
        if (collection == null && throwIfNotFound) {
            throw new MongoServerException("Collection [" + getFullCollectionNamespace(collectionName) + "] not found.");
        }
        return collection;
    }

    private void checkCollectionName(String collectionName) {
        if (collectionName.length() > Constants.MAX_NS_LENGTH) {
            throw new MongoServerError(10080, "ns name too long, max size is " + Constants.MAX_NS_LENGTH);
        }

        if (collectionName.isEmpty()) {
            throw new MongoServerError(16256, "Invalid ns [" + collectionName + "]");
        }
    }

    @Override
    public boolean isEmpty() {
        return collections.isEmpty();
    }

    private void addNamespace(MongoCollection<P> collection) {
        collections.put(collection.getCollectionName(), collection);
        if (!isSystemCollection(collection.getCollectionName())) {
            namespaces.addDocument(new Document("name", collection.getFullName()));
        }
    }

    protected void addIndex(Document indexDescription) {
        if (!indexDescription.containsKey("v")) {
            indexDescription.put("v", 2);
        }
        openOrCreateIndex(indexDescription);
    }

    protected MongoCollection<P> getOrCreateIndexesCollection() {
        if (indexes.get() == null) {
            MongoCollection<P> indexCollection = openOrCreateCollection(INDEXES_COLLECTION_NAME, CollectionOptions.withoutIdField());
            addNamespace(indexCollection);
            indexes.set(indexCollection);
        }
        return indexes.get();
    }

    private String extractCollectionNameFromNamespace(String namespace) {
        Assert.startsWith(namespace, databaseName);
        return namespace.substring(databaseName.length() + 1);
    }

    private void openOrCreateIndex(Document indexDescription) {
        String ns = indexDescription.get("ns").toString();
        String collectionName = extractCollectionNameFromNamespace(ns);
        MongoCollection<P> collection = resolveOrCreateCollection(collectionName);
        Index<P> index = openOrCreateIndex(collectionName, indexDescription);
        MongoCollection<P> indexesCollection = getOrCreateIndexesCollection();
        if (index != null) {
            collection.addIndex(index);
            indexesCollection.addDocumentIfMissing(indexDescription);
        }
    }

    private Index<P> openOrCreateIndex(String collectionName, Document indexDescription) {
        String indexName = (String) indexDescription.get("name");
        Document key = (Document) indexDescription.get("key");
        if (isPrimaryKeyIndex(key)) {
            if (!indexName.equals(PRIMARY_KEY_INDEX_NAME)) {
                log.warn("Ignoring primary key index with name '{}'", indexName);
                return null;
            }
            boolean ascending = isAscending(key.get(ID_FIELD));
            Index<P> index = openOrCreateIdIndex(collectionName, indexName, ascending);
            log.info("adding unique _id index for collection {}", collectionName);
            return index;
        } else {
            List<IndexKey> keys = new ArrayList<>();
            for (Entry<String, Object> entry : key.entrySet()) {
                String field = entry.getKey();
                boolean ascending = isAscending(entry.getValue());
                keys.add(new IndexKey(field, ascending));
            }

            boolean sparse = Utils.isTrue(indexDescription.get("sparse"));
            if (Utils.isTrue(indexDescription.get("unique"))) {
                log.info("adding {} unique index {} for collection {}", sparse ? "sparse" : "non-sparse", keys, collectionName);

                return openOrCreateUniqueIndex(collectionName, indexName, keys, sparse);
            } else {
                return openOrCreateSecondaryIndex(collectionName, indexName, keys, sparse);
            }
        }
    }

    @VisibleForExternalBackends
    protected boolean isPrimaryKeyIndex(Document key) {
        return key.keySet().equals(Set.of(ID_FIELD));
    }

    @VisibleForExternalBackends
    protected Index<P> openOrCreateSecondaryIndex(String collectionName, String indexName, List<IndexKey> keys, boolean sparse) {
        log.warn("adding secondary index with keys {} is not yet implemented. ignoring", keys);
        return new EmptyIndex<>(indexName, keys);
    }

    private static boolean isAscending(Object keyValue) {
        return Objects.equals(Utils.normalizeValue(keyValue), Double.valueOf(1.0));
    }

    private Index<P> openOrCreateIdIndex(String collectionName, String indexName, boolean ascending) {
        return openOrCreateUniqueIndex(collectionName, indexName, List.of(new IndexKey(ID_FIELD, ascending)), false);
    }

    protected abstract Index<P> openOrCreateUniqueIndex(String collectionName, String indexName, List<IndexKey> keys, boolean sparse);

    private List<Document> insertDocuments(Channel channel, String collectionName, List<Document> documents, Oplog oplog, boolean isOrdered) {
        clearLastStatus(channel);
        try {
            if (isSystemCollection(collectionName)) {
                throw new MongoServerError(16459, "attempt to insert in system namespace");
            }
            MongoCollection<P> collection = resolveOrCreateCollection(collectionName);
            List<Document> writeErrors = collection.insertDocuments(documents, isOrdered);
            oplog.handleInsert(collection.getFullName(), documents);
            if (!writeErrors.isEmpty()) {
                Document writeError = new Document(writeErrors.get(0));
                writeError.put("err", writeError.remove("errmsg"));
                putLastResult(channel, writeError);
            } else {
                Document result = new Document("n", 0);
                result.put("err", null);
                putLastResult(channel, result);
            }
            return writeErrors;
        } catch (MongoServerError e) {
            putLastError(channel, e);
            return List.of(toWriteError(0, e));
        }
    }

    private Document deleteDocuments(Channel channel, String collectionName, Document selector, int limit, Oplog oplog) {
        clearLastStatus(channel);
        try {
            if (isSystemCollection(collectionName)) {
                throw new InvalidNamespaceError(getFullCollectionNamespace(collectionName));
            }
            MongoCollection<P> collection = resolveCollection(collectionName, false);
            final int n;
            if (collection == null) {
                n = 0;
            } else {
                n = collection.deleteDocuments(selector, limit, oplog);
            }
            Document result = new Document("n", Integer.valueOf(n));
            putLastResult(channel, result);
            return result;
        } catch (MongoServerError e) {
            putLastError(channel, e);
            throw e;
        }
    }

    private Document updateDocuments(String collectionName, Document selector,
                                     Document update, ArrayFilters arrayFilters,
                                     boolean multi, boolean upsert, Oplog oplog) {

        if (isSystemCollection(collectionName)) {
            throw new MongoServerError(10156, "cannot update system collection");
        }

        MongoCollection<P> collection = resolveOrCreateCollection(collectionName);
        return collection.updateDocuments(selector, update, arrayFilters, multi, upsert, oplog);
    }

    private void putLastError(Channel channel, MongoServerException ex) {
        Document error = toError(channel, ex);
        putLastResult(channel, error);
    }

    private Document toWriteError(int index, MongoServerException e) {
        Document error = new Document();
        error.put("index", index);
        error.put("errmsg", e.getMessageWithoutErrorCode());
        if (e instanceof MongoServerError) {
            MongoServerError err = (MongoServerError) e;
            error.put("code", Integer.valueOf(err.getCode()));
            error.putIfNotNull("codeName", err.getCodeName());
        }
        return error;
    }

    private Document toError(Channel channel, MongoServerException ex) {
        Document error = new Document();
        error.put("err", ex.getMessageWithoutErrorCode());
        if (ex instanceof MongoServerError) {
            MongoServerError err = (MongoServerError) ex;
            error.put("code", Integer.valueOf(err.getCode()));
            error.putIfNotNull("codeName", err.getCodeName());
        }
        error.put("connectionId", channel.id().asShortText());
        return error;
    }

    protected void putLastResult(Channel channel, Document result) {
        List<Document> results = lastResults.get(channel);
        // list must not be empty
        Document last = results.get(results.size() - 1);
        Assert.isNull(last, () -> "last result already set: " + last);
        results.set(results.size() - 1, result);
    }

    private MongoCollection<P> createCollection(String collectionName, CollectionOptions options) {
        checkCollectionName(collectionName);
        if (collectionName.contains("$")) {
            throw new MongoServerError(10093, "cannot insert into reserved $ collection");
        }

        MongoCollection<P> collection = openOrCreateCollection(collectionName, options);
        addNamespace(collection);

        addIndex(getPrimaryKeyIndexDescription(collection.getFullName()));

        log.info("created collection {}", collection.getFullName());

        return collection;
    }

    protected abstract MongoCollection<P> openOrCreateCollection(String collectionName, CollectionOptions options);

    @Override
    public void drop(Oplog oplog) {
        log.debug("dropping {}", this);
        for (String collectionName : collections.keySet()) {
            if (!isSystemCollection(collectionName)) {
                dropCollection(collectionName, oplog);
            }
        }
        dropCollectionIfExists(INDEXES_COLLECTION_NAME, oplog);
        dropCollectionIfExists(NAMESPACES_COLLECTION_NAME, oplog);
    }

    private void dropCollectionIfExists(String collectionName, Oplog oplog) {
        if (collections.containsKey(collectionName)) {
            dropCollection(collectionName, oplog);
        }
    }

    @Override
    public void dropCollection(String collectionName, Oplog oplog) {
        MongoCollection<P> collection = resolveCollection(collectionName, true);
        dropAllIndexes(collection);
        collection.drop();
        unregisterCollection(collectionName);
        oplog.handleDropCollection(String.format("%s.%s", databaseName, collectionName));
    }

    private void dropAllIndexes(MongoCollection<P> collection) {
        MongoCollection<P> indexCollection = indexes.get();
        if (indexCollection == null) {
            return;
        }
        List<Document> indexesToDrop = new ArrayList<>();
        for (Document index : indexCollection.handleQuery(new Document("ns", collection.getFullName()))) {
            indexesToDrop.add(index);
        }
        for (Document indexToDrop : indexesToDrop) {
            dropIndex(collection, indexToDrop);
        }
    }

    @Override
    public void unregisterCollection(String collectionName) {
        MongoCollection<P> removedCollection = collections.remove(collectionName);
        namespaces.deleteDocuments(new Document("name", removedCollection.getFullName()), 1);
    }

    @Override
    public void moveCollection(MongoDatabase oldDatabase, MongoCollection<?> collection, String newCollectionName) {
        String oldFullName = collection.getFullName();
        oldDatabase.unregisterCollection(collection.getCollectionName());
        collection.renameTo(this, newCollectionName);
        // TODO resolve cast
        @SuppressWarnings("unchecked")
        MongoCollection<P> newCollection = (MongoCollection<P>) collection;
        MongoCollection<P> oldCollection = collections.put(newCollectionName, newCollection);
        Assert.isNull(oldCollection,
            () -> "Failed to register renamed collection. Another collection still existed: " + oldCollection);
        List<Document> newDocuments = new ArrayList<>();
        newDocuments.add(new Document("name", collection.getFullName()));

        indexes.get().updateDocuments(new Document("ns", oldFullName),
            new Document("$set", new Document("ns", newCollection.getFullName())),
            ArrayFilters.empty(), true, false, NoopOplog.get());

        namespaces.insertDocuments(newDocuments, true);
    }

    protected String getFullCollectionNamespace(String collectionName) {
        return getDatabaseName() + "." + collectionName;
    }

    static boolean isSystemCollection(String collectionName) {
        return collectionName.startsWith("system.");
    }

}
