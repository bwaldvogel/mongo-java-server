package de.bwaldvogel.mongo.backend;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.exception.MongoServerException;
import de.bwaldvogel.mongo.exception.MongoSilentServerException;
import de.bwaldvogel.mongo.exception.NoSuchCollectionException;
import de.bwaldvogel.mongo.exception.NoSuchCommandException;
import de.bwaldvogel.mongo.wire.message.MongoDelete;
import de.bwaldvogel.mongo.wire.message.MongoInsert;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import de.bwaldvogel.mongo.wire.message.MongoUpdate;
import io.netty.channel.Channel;

public abstract class AbstractMongoDatabase<P> implements MongoDatabase {

    private static final String NAMESPACES_COLLECTION_NAME = "system.namespaces";

    private static final String INDEXES_COLLECTION_NAME = "system.indexes";

    private static final Logger log = LoggerFactory.getLogger(AbstractMongoDatabase.class);

    protected final String databaseName;
    private final MongoBackend backend;

    private final Map<String, MongoCollection<P>> collections = new ConcurrentHashMap<>();

    private final AtomicReference<MongoCollection<P>> indexes = new AtomicReference<>();

    private final Map<Channel, List<Document>> lastResults = new ConcurrentHashMap<>();

    private MongoCollection<P> namespaces;

    protected AbstractMongoDatabase(String databaseName, MongoBackend backend) {
        this.databaseName = databaseName;
        this.backend = backend;
    }

    protected void initializeNamespacesAndIndexes() throws MongoServerException {
        this.namespaces = openOrCreateCollection(NAMESPACES_COLLECTION_NAME, "name");
        this.collections.put(namespaces.getCollectionName(), namespaces);

        if (this.namespaces.count() > 0) {
            for (Document namespace : namespaces.handleQuery(new Document(), 0, 0, null)) {
                String name = namespace.get("name").toString();
                log.debug("opening {}", name);
                String collectionName = extractCollectionNameFromNamespace(name);
                MongoCollection<P> collection = openOrCreateCollection(collectionName, Constants.ID_FIELD);
                collections.put(collectionName, collection);
                log.debug("opened collection '{}'", collectionName);
            }

            MongoCollection<P> indexCollection = collections.get(INDEXES_COLLECTION_NAME);
            indexes.set(indexCollection);
            for (Document indexDescription : indexCollection.handleQuery(new Document(), 0, 0, null)) {
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

    private Document commandDropDatabase() throws MongoServerException {
        backend.dropDatabase(getDatabaseName());
        Document response = new Document("dropped", getDatabaseName());
        Utils.markOkay(response);
        return response;
    }

    @Override
    public Document handleCommand(Channel channel, String command, Document query) throws MongoServerException {

        // getlasterror must not clear the last error
        if (command.equalsIgnoreCase("getlasterror")) {
            return commandGetLastError(channel, command, query);
        } else if (command.equalsIgnoreCase("getpreverror")) {
            return commandGetPrevError(channel, command, query);
        } else if (command.equalsIgnoreCase("reseterror")) {
            return commandResetError(channel, command, query);
        }

        clearLastStatus(channel);

        if (command.equalsIgnoreCase("insert")) {
            return commandInsert(channel, command, query);
        } else if (command.equalsIgnoreCase("update")) {
            return commandUpdate(channel, command, query);
        } else if (command.equalsIgnoreCase("delete")) {
            return commandDelete(channel, command, query);
        } else if (command.equalsIgnoreCase("create")) {
            return commandCreate(channel, command, query);
        } else if (command.equalsIgnoreCase("createIndexes")) {
            return commandCreateIndexes(channel, command, query);
        } else if (command.equalsIgnoreCase("count")) {
            return commandCount(command, query);
        } else if (command.equalsIgnoreCase("distinct")) {
            String collectionName = query.get(command).toString();
            MongoCollection<P> collection = resolveCollection(collectionName, true);
            return collection.handleDistinct(query);
        } else if (command.equalsIgnoreCase("drop")) {
            return commandDrop(query);
        } else if (command.equalsIgnoreCase("dropDatabase")) {
            return commandDropDatabase();
        } else if (command.equalsIgnoreCase("dbstats")) {
            return commandDatabaseStats();
        } else if (command.equalsIgnoreCase("collstats")) {
            String collectionName = query.get(command).toString();
            MongoCollection<P> collection = resolveCollection(collectionName, true);
            return collection.getStats();
        } else if (command.equalsIgnoreCase("validate")) {
            String collectionName = query.get(command).toString();
            MongoCollection<P> collection = resolveCollection(collectionName, true);
            return collection.validate();
        } else if (command.equalsIgnoreCase("findAndModify")) {
            String collectionName = query.get(command).toString();
            MongoCollection<P> collection = resolveOrCreateCollection(collectionName);
            return collection.findAndModify(query);
        } else if (command.equalsIgnoreCase("listCollections")) {
            return listCollections();
        } else if (command.equalsIgnoreCase("listIndexes")) {
            return listIndexes();
        } else {
            log.error("unknown query: {}", query);
        }
        throw new NoSuchCommandException(command);
    }

    private Document listCollections() throws MongoServerException {
        Document cursor = new Document();
        cursor.put("id", Long.valueOf(0));
        cursor.put("ns", getDatabaseName() + ".$cmd.listCollections");

        List<Document> firstBatch = new ArrayList<>();
        for (Document collection : namespaces.handleQuery(new Document(), 0, 0, null)) {
            Document collectionDescription = new Document();
            Document collectionOptions = new Document();
            String namespace = (String) collection.get("name");
            String collectionName = extractCollectionNameFromNamespace(namespace);
            collectionDescription.put("name", collectionName);
            collectionDescription.put("options", collectionOptions);
            firstBatch.add(collectionDescription);
        }

        cursor.put("firstBatch", firstBatch);

        Document response = new Document();
        response.put("cursor", cursor);
        Utils.markOkay(response);
        return response;
    }

    private Document listIndexes() throws MongoServerException {
        MongoCollection<P> indexes = resolveCollection(INDEXES_COLLECTION_NAME, true);

        Document cursor = new Document();
        cursor.put("id", Long.valueOf(0));
        cursor.put("ns", getDatabaseName() + ".$cmd.listIndexes");

        List<Document> firstBatch = new ArrayList<>();
        for (Document description : indexes.handleQuery(new Document(), 0, 0, null)) {
            firstBatch.add(description);
        }

        cursor.put("firstBatch", firstBatch);

        Document response = new Document();
        response.put("cursor", cursor);
        Utils.markOkay(response);
        return response;
    }

    private synchronized MongoCollection<P> resolveOrCreateCollection(final String collectionName) throws MongoServerException {
        final MongoCollection<P> collection = resolveCollection(collectionName, false);
        if (collection != null) {
            return collection;
        } else {
            return createCollection(collectionName);
        }
    }

    private Document commandInsert(Channel channel, String command, Document query) throws MongoServerException {
        String collectionName = query.get(command).toString();
        boolean isOrdered = Utils.isTrue(query.get("ordered"));
        log.trace("ordered: {}", isOrdered);

        @SuppressWarnings("unchecked")
        List<Document> documents = (List<Document>) query.get("documents");

        List<Document> writeErrors = new ArrayList<>();
        int n = 0;
        for (Document document : documents) {
            try {
                insertDocuments(channel, collectionName, Collections.singletonList(document));
                n++;
            } catch (MongoServerError e) {
                Document error = new Document();
                error.put("index", Integer.valueOf(n));
                error.put("code", Integer.valueOf(e.getCode()));
                error.put("errmsg", e.getMessage());
                writeErrors.add(error);
            }
        }
        Document result = new Document();
        result.put("n", Integer.valueOf(n));
        if (!writeErrors.isEmpty()) {
            result.put("writeErrors", writeErrors);
        }
        // odd by true: also mark error as okay
        Utils.markOkay(result);
        return result;
    }

    private Document commandUpdate(Channel channel, String command, Document query) throws MongoServerException {
        String collectionName = query.get(command).toString();
        boolean isOrdered = Utils.isTrue(query.get("ordered"));
        log.trace("ordered: {}", isOrdered);

        @SuppressWarnings("unchecked")
        List<Document> updates = (List<Document>) query.get("updates");
        int nMatched = 0;
        int nModified = 0;
        int nUpserted = 0;
        Collection<Document> upserts = new ArrayList<>();
        for (Document updateObj : updates) {
            Document selector = (Document) updateObj.get("q");
            Document update = (Document) updateObj.get("u");
            boolean multi = Utils.isTrue(updateObj.get("multi"));
            boolean upsert = Utils.isTrue(updateObj.get("upsert"));
            final Document result = updateDocuments(channel, collectionName, selector, update, multi, upsert);
            if (result.containsKey("upserted")) {
                final Object id = result.get("upserted");
                final Document upserted = new Document("index", upserts.size());
                upserted.put("_id", id);
                upserts.add(upserted);
            }
            nMatched += ((Integer) result.get("n")).intValue();
            nModified += ((Integer) result.get("nModified")).intValue();
        }

        Document response = new Document();
        response.put("n", nMatched);
        response.put("nModified", nModified);
        if (!upserts.isEmpty()) {
            response.put("upserted", upserts);
        }
        Utils.markOkay(response);
        putLastResult(channel, response);
        return response;
    }

    private Document commandDelete(Channel channel, String command, Document query) throws MongoServerException {
        String collectionName = query.get(command).toString();
        boolean isOrdered = Utils.isTrue(query.get("ordered"));
        log.trace("ordered: {}", isOrdered);

        @SuppressWarnings("unchecked")
        List<Document> deletes = (List<Document>) query.get("deletes");
        int n = 0;
        for (Document delete : deletes) {
            final Document selector = (Document) delete.get("q");
            final int limit = ((Number) delete.get("limit")).intValue();
            Document result = deleteDocuments(channel, collectionName, selector, limit);
            Integer resultNumber = (Integer) result.get("n");
            n += resultNumber.intValue();
        }

        Document response = new Document("n", Integer.valueOf(n));
        Utils.markOkay(response);
        return response;
    }

    private Document commandCreate(Channel channel, String command, Document query) throws MongoServerException {
        String collectionName = query.get(command).toString();
        boolean isCapped = Utils.isTrue(query.get("capped"));
        if (isCapped) {
            throw new MongoServerException("Creating capped collections is not yet implemented");
        }

        Object autoIndexId = query.get("autoIndexId");
        if (autoIndexId != null && !Utils.isTrue(autoIndexId)) {
            throw new MongoServerException("Disabling autoIndexId is not yet implemented");
        }

        MongoCollection<P> collection = resolveCollection(collectionName, false);
        if (collection != null) {
            throw new MongoServerError(48, "collection already exists");
        }

        createCollection(collectionName);

        Document response = new Document();
        Utils.markOkay(response);
        return response;
    }

    private Document commandCreateIndexes(Channel channel, String command, Document query) throws MongoServerException {

        int indexesBefore = countIndexes();

        @SuppressWarnings("unchecked")
        final Collection<Document> indexDescriptions = (Collection<Document>) query.get("indexes");
        for (Document indexDescription : indexDescriptions) {
            addIndex(indexDescription);
        }

        int indexesAfter = countIndexes();

        Document response = new Document();
        response.put("numIndexesBefore", Integer.valueOf(indexesBefore));
        response.put("numIndexesAfter", Integer.valueOf(indexesAfter));
        Utils.markOkay(response);
        return response;
    }

    private int countIndexes() {
        final MongoCollection<P> indexesCollection;
        synchronized (indexes) {
            indexesCollection = indexes.get();
        }
        if (indexesCollection == null) {
            return 0;
        } else {
            return indexesCollection.count();
        }
    }

    private Document commandDatabaseStats() throws MongoServerException {
        Document response = new Document("db", getDatabaseName());
        response.put("collections", Integer.valueOf(namespaces.count()));

        long storageSize = getStorageSize();
        long fileSize = getFileSize();
        long indexSize = 0;
        long objects = 0;
        long dataSize = 0;
        double averageObjectSize = 0;

        for (MongoCollection<P> collection : collections.values()) {
            Document stats = collection.getStats();
            objects += ((Number) stats.get("count")).longValue();
            dataSize += ((Number) stats.get("size")).longValue();

            Document indexSizes = (Document) stats.get("indexSize");
            for (String indexName : indexSizes.keySet()) {
                indexSize += ((Number) indexSizes.get(indexName)).longValue();
            }

        }
        if (objects > 0) {
            averageObjectSize = dataSize / ((double) objects);
        }
        response.put("objects", Long.valueOf(objects));
        response.put("avgObjSize", Double.valueOf(averageObjectSize));
        response.put("dataSize", Long.valueOf(dataSize));
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

    private Document commandDrop(Document query) throws MongoServerException {
        String collectionName = query.get("drop").toString();
        MongoCollection<P> collection = collections.remove(collectionName);

        if (collection == null) {
            throw new MongoSilentServerException("ns not found");
        }
        Document response = new Document();
        namespaces.removeDocument(new Document("name", collection.getFullName()));
        response.put("nIndexesWas", Integer.valueOf(collection.getNumIndexes()));
        response.put("ns", collection.getFullName());
        Utils.markOkay(response);
        return response;

    }

    private Document commandGetLastError(Channel channel, String command, Document query) throws MongoServerException {
        Iterator<String> it = query.keySet().iterator();
        String cmd = it.next();
        if (!cmd.equals(command)) {
            throw new IllegalStateException();
        }
        if (it.hasNext()) {
            String subCommand = it.next();
            switch (subCommand) {
                case "w":
                    // ignore
                    break;
                case "fsync":
                    // ignore
                    break;
                default:
                    throw new MongoServerException("unknown subcommand: " + subCommand);
            }
        }

        List<Document> results = lastResults.get(channel);

        Document result = null;
        if (results != null && !results.isEmpty()) {
            result = results.get(results.size() - 1);
            if (result == null) {
                result = new Document();
            }
        } else {
            result = new Document();
            result.put("err", null);
        }
        Utils.markOkay(result);
        return result;
    }

    private Document commandGetPrevError(Channel channel, String command, Document query) {
        List<Document> results = lastResults.get(channel);

        if (results != null) {
            for (int i = 1; i < results.size(); i++) {
                Document result = results.get(results.size() - i);
                if (result == null) {
                    continue;
                }

                boolean isRelevant = false;
                if (result.get("err") != null) {
                    isRelevant = true;
                } else if (((Number) result.get("n")).intValue() > 0) {
                    isRelevant = true;
                }

                if (isRelevant) {
                    result.put("nPrev", Integer.valueOf(i));
                    return result;
                }
            }
        }

        // found no prev error
        Document result = new Document();
        result.put("nPrev", Integer.valueOf(-1));
        Utils.markOkay(result);
        return result;
    }

    private Document commandResetError(Channel channel, String command, Document query) {
        List<Document> results = lastResults.get(channel);
        if (results != null) {
            results.clear();
        }
        Document result = new Document();
        Utils.markOkay(result);
        return result;
    }

    private Document commandCount(String command, Document query) throws MongoServerException {
        String collection = query.get(command).toString();
        Document response = new Document();
        MongoCollection<P> coll = collections.get(collection);
        if (coll == null) {
            response.put("missing", Boolean.TRUE);
            response.put("n", Integer.valueOf(0));
        } else {
            Document queryObject = (Document) query.get("query");
            int limit = getOptionalNumber(query, "limit", -1);
            int skip = getOptionalNumber(query, "skip", 0);
            response.put("n", Integer.valueOf(coll.count(queryObject, skip, limit)));
        }
        Utils.markOkay(response);
        return response;
    }

    private int getOptionalNumber(Document query, String fieldName, int defaultValue) {
        Number limitNumber = (Number) query.get(fieldName);
        return limitNumber != null ? limitNumber.intValue() : defaultValue;
    }

    @Override
    public Iterable<Document> handleQuery(MongoQuery query) throws MongoServerException {
        clearLastStatus(query.getChannel());
        String collectionName = query.getCollectionName();
        MongoCollection<P> collection = resolveCollection(collectionName, false);
        if (collection == null) {
            return Collections.emptyList();
        }
        int numSkip = query.getNumberToSkip();
        int numReturn = query.getNumberToReturn();
        Document fieldSelector = query.getReturnFieldSelector();
        return collection.handleQuery(query.getQuery(), numSkip, numReturn, fieldSelector);
    }

    @Override
    public void handleClose(Channel channel) {
        lastResults.remove(channel);
    }

    private synchronized void clearLastStatus(Channel channel) {
        List<Document> results = lastResults.get(channel);
        if (results == null) {
            results = new LimitedList<>(10);
            lastResults.put(channel, results);
        }
        results.add(null);
    }

    @Override
    public void handleInsert(MongoInsert insert) throws MongoServerException {
        Channel channel = insert.getChannel();
        String collectionName = insert.getCollectionName();
        final List<Document> documents = insert.getDocuments();

        if (collectionName.equals(INDEXES_COLLECTION_NAME)) {
            for (Document indexDescription : documents) {
                addIndex(indexDescription);
            }
        } else {
            try {
                insertDocuments(channel, collectionName, documents);
            } catch (MongoServerException e) {
                log.error("failed to insert {}", insert, e);
            }
        }
    }

    @Override
    public synchronized MongoCollection<P> resolveCollection(String collectionName, boolean throwIfNotFound) throws MongoServerException {
        checkCollectionName(collectionName);
        MongoCollection<P> collection = collections.get(collectionName);
        if (collection == null && throwIfNotFound) {
            throw new NoSuchCollectionException(collectionName);
        }
        return collection;
    }

    private void checkCollectionName(String collectionName) throws MongoServerException {

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

    private void addNamespace(MongoCollection<P> collection) throws MongoServerException {
        collections.put(collection.getCollectionName(), collection);
        namespaces.addDocument(new Document("name", collection.getFullName()));
    }

    @Override
    public void handleDelete(MongoDelete delete) throws MongoServerException {
        Channel channel = delete.getChannel();
        final String collectionName = delete.getCollectionName();
        final Document selector = delete.getSelector();
        final int limit = delete.isSingleRemove() ? 1 : Integer.MAX_VALUE;

        try {
            deleteDocuments(channel, collectionName, selector, limit);
        } catch (MongoServerException e) {
            log.error("failed to delete {}", delete, e);
        }
    }

    @Override
    public void handleUpdate(MongoUpdate updateCommand) throws MongoServerException {
        final Channel channel = updateCommand.getChannel();
        final String collectionName = updateCommand.getCollectionName();
        final Document selector = updateCommand.getSelector();
        final Document update = updateCommand.getUpdate();
        final boolean multi = updateCommand.isMulti();
        final boolean upsert = updateCommand.isUpsert();

        try {
            Document result = updateDocuments(channel, collectionName, selector, update, multi, upsert);
            putLastResult(channel, result);
        } catch (MongoServerException e) {
            log.error("failed to update {}", updateCommand, e);
        }
    }

    private void addIndex(Document indexDescription) throws MongoServerException {
        openOrCreateIndex(indexDescription);
        getOrCreateIndexesCollection().addDocument(indexDescription);
    }

    private MongoCollection<P> getOrCreateIndexesCollection() throws MongoServerException {
        synchronized(indexes) {
            if (indexes.get() == null) {
                MongoCollection<P> indexCollection = openOrCreateCollection(INDEXES_COLLECTION_NAME, null);
                addNamespace(indexCollection);
                indexes.set(indexCollection);
            }
            return indexes.get();
        }
    }

    private String extractCollectionNameFromNamespace(String namespace) {
        if (!namespace.startsWith(databaseName)) {
            throw new IllegalArgumentException();
        }
        final String collectionName = namespace.substring(databaseName.length() + 1);
        return collectionName;
    }

    private void openOrCreateIndex(Document indexDescription) throws MongoServerException {
        String ns = indexDescription.get("ns").toString();
        String collectionName = extractCollectionNameFromNamespace(ns);

        MongoCollection<P> collection = resolveOrCreateCollection(collectionName);

        Document key = (Document) indexDescription.get("key");
        if (key.keySet().equals(Collections.singleton("_id"))) {
            boolean ascending = Utils.normalizeValue(key.get("_id")).equals(Double.valueOf(1.0));
            collection.addIndex(openOrCreateUniqueIndex(collectionName, "_id", ascending));
            log.info("adding unique _id index for collection {}", collectionName);
        } else if (Utils.isTrue(indexDescription.get("unique"))) {
            if (key.keySet().size() != 1) {
                throw new MongoServerException("Compound unique indices are not yet implemented");
            }

            log.info("adding unique index {} for collection {}", key.keySet(), collectionName);

            final String field = key.keySet().iterator().next();
            boolean ascending = Utils.normalizeValue(key.get(field)).equals(Double.valueOf(1.0));
            collection.addIndex(openOrCreateUniqueIndex(collectionName, field, ascending));
        } else {
            // TODO: non-unique non-id indexes not yet implemented
            log.warn("adding non-unique non-id index with key {} is not yet implemented", key);
        }
    }

    protected abstract Index<P> openOrCreateUniqueIndex(String collectionName, String key, boolean ascending);

    private Document insertDocuments(final Channel channel, final String collectionName, final List<Document> documents) throws MongoServerException {
        clearLastStatus(channel);
        try {
            if (collectionName.startsWith("system.")) {
                throw new MongoServerError(16459, "attempt to insert in system namespace");
            }
            final MongoCollection<P> collection = resolveOrCreateCollection(collectionName);
            int n = collection.insertDocuments(documents);
            assert n == documents.size();
            final Document result = new Document("n", Integer.valueOf(n));
            putLastResult(channel, result);
            return result;
        } catch (MongoServerError e) {
            putLastError(channel, e);
            throw e;
        }
    }

    private Document deleteDocuments(final Channel channel, final String collectionName, final Document selector, final int limit) throws MongoServerException {
        clearLastStatus(channel);
        try {
            if (collectionName.startsWith("system.")) {
                throw new MongoServerError(12050, "cannot delete from system namespace");
            }
            MongoCollection<P> collection = resolveCollection(collectionName, false);
            int n;
            if (collection == null) {
                n = 0;
            } else {
                n = collection.deleteDocuments(selector, limit);
            }
            final Document result = new Document("n", Integer.valueOf(n));
            putLastResult(channel, result);
            return result;
        } catch (MongoServerError e) {
            putLastError(channel, e);
            throw e;
        }
    }

    private Document updateDocuments(final Channel channel, final String collectionName, final Document selector,
                                       final Document update, final boolean multi, final boolean upsert) throws MongoServerException {
        clearLastStatus(channel);
        try {
            if (collectionName.startsWith("system.")) {
                throw new MongoServerError(10156, "cannot update system collection");
            }

            MongoCollection<P> collection = resolveOrCreateCollection(collectionName);
            return collection.updateDocuments(selector, update, multi, upsert);
        } catch (MongoServerException e) {
            putLastError(channel, e);
            throw e;
        }
    }

    private void putLastError(Channel channel, MongoServerException ex) {
        Document error = new Document();
        if (ex instanceof MongoServerError) {
            MongoServerError err = (MongoServerError) ex;
            error.put("err", err.getMessage());
            error.put("code", Integer.valueOf(err.getCode()));
        } else {
            error.put("err", ex.getMessage());
        }
        // TODO: https://github.com/netty/netty/issues/1810
        // also note:
        // http://stackoverflow.com/questions/17690094/channel-id-has-been-removed-in-netty4-0-final-version-how-can-i-solve
        error.put("connectionId", Integer.valueOf(channel.hashCode()));
        putLastResult(channel, error);
    }

    private synchronized void putLastResult(Channel channel, Document result) {
        List<Document> results = lastResults.get(channel);
        // list must not be empty
        Document last = results.get(results.size() - 1);
        if (last != null) {
            throw new IllegalStateException("last result already set: " + last);
        }
        results.set(results.size() - 1, result);
    }

    private MongoCollection<P> createCollection(String collectionName) throws MongoServerException {
        checkCollectionName(collectionName);
        if (collectionName.contains("$")) {
            throw new MongoServerError(10093, "cannot insert into reserved $ collection");
        }

        MongoCollection<P> collection = openOrCreateCollection(collectionName, Constants.ID_FIELD);
        addNamespace(collection);

        Document indexDescription = new Document();
        indexDescription.put("name", "_id_");
        indexDescription.put("ns", collection.getFullName());
        indexDescription.put("key", new Document("_id", Integer.valueOf(1)));
        addIndex(indexDescription);

        log.info("created collection {}", collection.getFullName());

        return collection;
    }

    protected abstract MongoCollection<P> openOrCreateCollection(String collectionName, String idField);

    @Override
    public void drop() throws MongoServerException {
        log.debug("dropping {}", this);
        for (String collectionName : collections.keySet()) {
            dropCollection(collectionName);
        }
    }

    @Override
    public void dropCollection(String collectionName) throws MongoServerException {
        deregisterCollection(collectionName);
    }

    @Override
    public MongoCollection<P> deregisterCollection(String collectionName) throws MongoServerException {
        MongoCollection<P> removedCollection = collections.remove(collectionName);
        namespaces.deleteDocuments(new Document("name", removedCollection.getFullName()), 1);
        return removedCollection;
    }

    @Override
    public void moveCollection(MongoDatabase oldDatabase, MongoCollection<?> collection, String newCollectionName)
            throws MongoServerException {
        oldDatabase.deregisterCollection(collection.getCollectionName());
        collection.renameTo(getDatabaseName(), newCollectionName);
        // TODO resolve cast
        @SuppressWarnings("unchecked")
        MongoCollection<P> newCollection = (MongoCollection<P>) collection;
        collections.put(newCollectionName, newCollection);
        List<Document> newDocuments = new ArrayList<>();
        newDocuments.add(new Document("name", collection.getFullName()));
        namespaces.insertDocuments(newDocuments);
    }

}
