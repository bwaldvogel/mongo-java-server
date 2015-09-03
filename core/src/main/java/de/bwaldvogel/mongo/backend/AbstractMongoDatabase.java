package de.bwaldvogel.mongo.backend;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.MongoDatabase;
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

public abstract class AbstractMongoDatabase<KEY> implements MongoDatabase {

    private static final String NAMESPACES_COLLECTION_NAME = "system.namespaces";

    private static final String INDEXES_COLLECTION_NAME = "system.indexes";

    private static final Logger log = LoggerFactory.getLogger(AbstractMongoDatabase.class);

    protected final String databaseName;
    private final MongoBackend backend;

    private final Map<String, MongoCollection<KEY>> collections = new ConcurrentHashMap<String, MongoCollection<KEY>>();

    private final AtomicReference<MongoCollection<KEY>> indexes = new AtomicReference<MongoCollection<KEY>>();

    private final Map<Channel, List<BSONObject>> lastResults = new ConcurrentHashMap<Channel, List<BSONObject>>();

    private MongoCollection<KEY> namespaces;

    protected AbstractMongoDatabase(String databaseName, MongoBackend backend) {
        this.databaseName = databaseName;
        this.backend = backend;
    }

    protected void initializeNamespacesAndIndexes() throws MongoServerException {
        this.namespaces = openOrCreateCollection(NAMESPACES_COLLECTION_NAME, "name");
        this.collections.put(namespaces.getCollectionName(), namespaces);

        if (this.namespaces.count() > 0) {
            for (BSONObject namespace : namespaces.handleQuery(new BasicDBObject(), 0, 0, null)) {
                String name = namespace.get("name").toString();
                log.debug("opening {}", name);
                String collectionName = extractCollectionNameFromNamespace(name);
                MongoCollection<KEY> collection = openOrCreateCollection(collectionName, Constants.ID_FIELD);
                collections.put(collectionName, collection);
                log.debug("opened collection '{}'", collectionName);
            }

            MongoCollection<KEY> indexCollection = collections.get(INDEXES_COLLECTION_NAME);
            indexes.set(indexCollection);
            for (BSONObject indexDescription : indexCollection.handleQuery(new BasicDBObject(), 0, 0, null)) {
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

    private BSONObject commandDropDatabase() throws MongoServerException {
        backend.dropDatabase(getDatabaseName());
        BSONObject response = new BasicBSONObject("dropped", getDatabaseName());
        Utils.markOkay(response);
        return response;
    }

    @Override
    public BSONObject handleCommand(Channel channel, String command, BSONObject query) throws MongoServerException {

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
            MongoCollection<KEY> collection = resolveCollection(collectionName, true);
            return collection.handleDistinct(query);
        } else if (command.equalsIgnoreCase("drop")) {
            return commandDrop(query);
        } else if (command.equalsIgnoreCase("dropDatabase")) {
            return commandDropDatabase();
        } else if (command.equalsIgnoreCase("dbstats")) {
            return commandDatabaseStats();
        } else if (command.equalsIgnoreCase("collstats")) {
            String collectionName = query.get(command).toString();
            MongoCollection<KEY> collection = resolveCollection(collectionName, true);
            return collection.getStats();
        } else if (command.equalsIgnoreCase("validate")) {
            String collectionName = query.get(command).toString();
            MongoCollection<KEY> collection = resolveCollection(collectionName, true);
            return collection.validate();
        } else if (command.equalsIgnoreCase("findAndModify")) {
            String collectionName = query.get(command).toString();
            MongoCollection<KEY> collection = resolveOrCreateCollection(collectionName);
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

    protected BSONObject listCollections() throws MongoServerException {
        BSONObject cursor = new BasicBSONObject();
        cursor.put("id", Long.valueOf(0));
        cursor.put("ns", getDatabaseName() + ".$cmd.listCollections");

        List<BSONObject> firstBatch = new ArrayList<BSONObject>();
        for (BSONObject collection : namespaces.handleQuery(new BasicBSONObject(), 0, 0, null)) {
            BSONObject collectionDescription = new BasicBSONObject();
            BSONObject collectionOptions = new BasicBSONObject();
            String namespace = (String) collection.get("name");
            String collectionName = extractCollectionNameFromNamespace(namespace);
            collectionDescription.put("name", collectionName);
            collectionDescription.put("options", collectionOptions);
            firstBatch.add(collectionDescription);
        }

        cursor.put("firstBatch", firstBatch);

        BSONObject response = new BasicBSONObject();
        response.put("cursor", cursor);
        Utils.markOkay(response);
        return response;
    }

    protected BSONObject listIndexes() throws MongoServerException {
        MongoCollection<KEY> indexes = resolveCollection(INDEXES_COLLECTION_NAME, true);

        BSONObject cursor = new BasicBSONObject();
        cursor.put("id", Long.valueOf(0));
        cursor.put("ns", getDatabaseName() + ".$cmd.listIndexes");

        List<BSONObject> firstBatch = new ArrayList<BSONObject>();
        for (BSONObject description : indexes.handleQuery(new BasicBSONObject(), 0, 0, null)) {
            firstBatch.add(description);
        }

        cursor.put("firstBatch", firstBatch);

        BSONObject response = new BasicBSONObject();
        response.put("cursor", cursor);
        Utils.markOkay(response);
        return response;
    }

    protected MongoCollection<KEY> resolveOrCreateCollection(final String collectionName) throws MongoServerException {
        final MongoCollection<KEY> collection = resolveCollection(collectionName, false);
        if (collection != null) {
            return collection;
        } else {
            return createCollection(collectionName);
        }
    }

    protected BSONObject commandInsert(Channel channel, String command, BSONObject query) throws MongoServerException {
        String collectionName = query.get(command).toString();
        boolean isOrdered = Utils.isTrue(query.get("ordered"));
        log.trace("ordered: {}", isOrdered);

        @SuppressWarnings("unchecked")
        List<BSONObject> documents = (List<BSONObject>) query.get("documents");

        List<BSONObject> writeErrors = new ArrayList<BSONObject>();
        int n = 0;
        for (BSONObject document : documents) {
            try {
                insertDocuments(channel, collectionName, Arrays.asList(document));
                n++;
            } catch (MongoServerError e) {
                BSONObject error = new BasicBSONObject();
                error.put("index", Integer.valueOf(n));
                error.put("code", Integer.valueOf(e.getCode()));
                error.put("errmsg", e.getMessage());
                writeErrors.add(error);
            }
        }
        BSONObject result = new BasicBSONObject();
        result.put("n", Integer.valueOf(n));
        if (!writeErrors.isEmpty()) {
            result.put("writeErrors", writeErrors);
        }
        // odd by true: also mark error as okay
        Utils.markOkay(result);
        return result;
    }

    protected BSONObject commandUpdate(Channel channel, String command, BSONObject query) throws MongoServerException {
        String collectionName = query.get(command).toString();
        boolean isOrdered = Utils.isTrue(query.get("ordered"));
        log.trace("ordered: {}", isOrdered);

        @SuppressWarnings("unchecked")
        List<BSONObject> updates = (List<BSONObject>) query.get("updates");
        int n = 0;
        boolean updatedExisting = false;
        Collection<BSONObject> upserts = new ArrayList<BSONObject>();
        for (BSONObject updateObj : updates) {
            BSONObject selector = (BSONObject) updateObj.get("q");
            BSONObject update = (BSONObject) updateObj.get("u");
            boolean multi = Utils.isTrue(updateObj.get("multi"));
            boolean upsert = Utils.isTrue(updateObj.get("upsert"));
            final BSONObject result = updateDocuments(channel, collectionName, selector, update, multi, upsert);
            updatedExisting |= Utils.isTrue(result.get("updatedExisting"));
            if (result.containsField("upserted")) {
                final Object id = result.get("upserted");
                final BSONObject upserted = new BasicBSONObject("index", upserts.size());
                upserted.put("_id", id);
                upserts.add(upserted);
            }
            Integer resultNumber = (Integer) result.get("n");
            n += resultNumber.intValue();
        }

        BSONObject response = new BasicBSONObject("n", Integer.valueOf(n));
        response.put("updatedExisting", Boolean.valueOf(updatedExisting));
        if (!upserts.isEmpty()) {
            response.put("upserted", upserts);
        }
        Utils.markOkay(response);
        putLastResult(channel, response);
        return response;
    }

    protected BSONObject commandDelete(Channel channel, String command, BSONObject query) throws MongoServerException {
        String collectionName = query.get(command).toString();
        boolean isOrdered = Utils.isTrue(query.get("ordered"));
        log.trace("ordered: {}", isOrdered);

        @SuppressWarnings("unchecked")
        List<BSONObject> deletes = (List<BSONObject>) query.get("deletes");
        int n = 0;
        for (BSONObject delete : deletes) {
            final BSONObject selector = (BSONObject) delete.get("q");
            final int limit = ((Number) delete.get("limit")).intValue();
            BSONObject result = deleteDocuments(channel, collectionName, selector, limit);
            Integer resultNumber = (Integer) result.get("n");
            n += resultNumber.intValue();
        }

        BSONObject response = new BasicBSONObject("n", Integer.valueOf(n));
        Utils.markOkay(response);
        return response;
   }

    protected BSONObject commandCreate(Channel channel, String command, BSONObject query) throws MongoServerException {
        String collectionName = query.get(command).toString();
        boolean isCapped = Utils.isTrue(query.get("capped"));
        if (isCapped) {
            throw new MongoServerException("Creating capped collections is not yet implemented");
        }

        Object autoIndexId = query.get("autoIndexId");
        if (autoIndexId != null && !Utils.isTrue(autoIndexId)) {
            throw new MongoServerException("Disabling autoIndexId is not yet implemented");
        }

        MongoCollection<KEY> collection = resolveCollection(collectionName, false);
        if (collection != null) {
            throw new MongoServerError(48, "collection already exists");
        }

        createCollection(collectionName);

        BSONObject response = new BasicBSONObject();
        Utils.markOkay(response);
        return response;
    }

    protected BSONObject commandCreateIndexes(Channel channel, String command, BSONObject query) throws MongoServerException {

        int indexesBefore = countIndexes();

        @SuppressWarnings("unchecked")
        final Collection<BSONObject> indexDescriptions = (Collection<BSONObject>) query.get("indexes");
        for (BSONObject indexDescription : indexDescriptions) {
            addIndex(indexDescription);
        }

        int indexesAfter = countIndexes();

        BSONObject response = new BasicBSONObject();
        response.put("numIndexesBefore", Integer.valueOf(indexesBefore));
        response.put("numIndexesAfter", Integer.valueOf(indexesAfter));
        Utils.markOkay(response);
        return response;
    }

    protected int countIndexes() {
        final MongoCollection<KEY> indexesCollection;
        synchronized (indexes) {
            indexesCollection = indexes.get();
        }
        if (indexesCollection == null) {
            return 0;
        } else {
            return indexesCollection.count();
        }
    }

    protected BSONObject commandDatabaseStats() throws MongoServerException {
        BSONObject response = new BasicBSONObject("db", getDatabaseName());
        response.put("collections", Integer.valueOf(namespaces.count()));

        long storageSize = getStorageSize();
        long fileSize = getFileSize();
        long indexSize = 0;
        long objects = 0;
        long dataSize = 0;
        double averageObjectSize = 0;

        for (MongoCollection<KEY> collection : collections.values()) {
            BSONObject stats = collection.getStats();
            objects += ((Number) stats.get("count")).longValue();
            dataSize += ((Number) stats.get("size")).longValue();

            BSONObject indexSizes = (BSONObject) stats.get("indexSize");
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

    protected BSONObject commandDrop(BSONObject query) throws MongoServerException {
        String collectionName = query.get("drop").toString();
        MongoCollection<KEY> collection = collections.remove(collectionName);

        if (collection == null) {
            throw new MongoSilentServerException("ns not found");
        }
        BSONObject response = new BasicBSONObject();
        namespaces.removeDocument(new BasicBSONObject("name", collection.getFullName()));
        response.put("nIndexesWas", Integer.valueOf(collection.getNumIndexes()));
        response.put("ns", collection.getFullName());
        Utils.markOkay(response);
        return response;

    }

    protected BSONObject commandGetLastError(Channel channel, String command, BSONObject query) throws MongoServerException {
        Iterator<String> it = query.keySet().iterator();
        String cmd = it.next();
        if (!cmd.equals(command)) {
            throw new IllegalStateException();
        }
        if (it.hasNext()) {
            String subCommand = it.next();
            if (subCommand.equals("w")) {
                // ignore
            } else if (subCommand.equals("fsync")) {
                // ignore
            } else {
                throw new MongoServerException("unknown subcommand: " + subCommand);
            }
        }

        List<BSONObject> results = lastResults.get(channel);

        BSONObject result = null;
        if (results != null && !results.isEmpty()) {
            result = results.get(results.size() - 1);
            if (result == null) {
                result = new BasicBSONObject();
            }
        } else {
            result = new BasicBSONObject();
            result.put("err", null);
        }
        Utils.markOkay(result);
        return result;
    }

    protected BSONObject commandGetPrevError(Channel channel, String command, BSONObject query) {
        List<BSONObject> results = lastResults.get(channel);

        if (results != null) {
            for (int i = 1; i < results.size(); i++) {
                BSONObject result = results.get(results.size() - i);
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
        BSONObject result = new BasicBSONObject();
        result.put("nPrev", Integer.valueOf(-1));
        Utils.markOkay(result);
        return result;
    }

    protected BSONObject commandResetError(Channel channel, String command, BSONObject query) {
        List<BSONObject> results = lastResults.get(channel);
        if (results != null) {
            results.clear();
        }
        BSONObject result = new BasicBSONObject();
        Utils.markOkay(result);
        return result;
    }

    protected BSONObject commandCount(String command, BSONObject query) throws MongoServerException {
        String collection = query.get(command).toString();
        BSONObject response = new BasicBSONObject();
        MongoCollection<KEY> coll = collections.get(collection);
        if (coll == null) {
            response.put("missing", Boolean.TRUE);
            response.put("n", Integer.valueOf(0));
        } else {
            response.put("n", Integer.valueOf(coll.count((BSONObject) query.get("query"))));
        }
        Utils.markOkay(response);
        return response;
    }

    @Override
    public Iterable<BSONObject> handleQuery(MongoQuery query) throws MongoServerException {
        clearLastStatus(query.getChannel());
        String collectionName = query.getCollectionName();
        MongoCollection<KEY> collection = resolveCollection(collectionName, false);
        if (collection == null) {
            return Collections.emptyList();
        }
        return collection.handleQuery(query.getQuery(), query.getNumberToSkip(), query.getNumberToReturn(),
                query.getReturnFieldSelector());
    }

    @Override
    public void handleClose(Channel channel) {
        lastResults.remove(channel);
    }

    public synchronized void clearLastStatus(Channel channel) {
        List<BSONObject> results = lastResults.get(channel);
        if (results == null) {
            results = new LimitedList<BSONObject>(10);
            lastResults.put(channel, results);
        }
        results.add(null);
    }

    @Override
    public void handleInsert(MongoInsert insert) throws MongoServerException {
        Channel channel = insert.getChannel();
        String collectionName = insert.getCollectionName();
        final List<BSONObject> documents = insert.getDocuments();

        if (collectionName.equals(INDEXES_COLLECTION_NAME)) {
            for (BSONObject indexDescription : documents) {
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
    public synchronized MongoCollection<KEY> resolveCollection(String collectionName, boolean throwIfNotFound) throws MongoServerException {
        checkCollectionName(collectionName);
        MongoCollection<KEY> collection = collections.get(collectionName);
        if (collection == null && throwIfNotFound) {
            throw new NoSuchCollectionException(collectionName);
        }
        return collection;
    }

    protected void checkCollectionName(String collectionName) throws MongoServerException {

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

    protected void addNamespace(MongoCollection<KEY> collection) throws MongoServerException {
        collections.put(collection.getCollectionName(), collection);
        namespaces.addDocument(new BasicDBObject("name", collection.getFullName()));
    }

    @Override
    public void handleDelete(MongoDelete delete) throws MongoServerException {
        Channel channel = delete.getChannel();
        final String collectionName = delete.getCollectionName();
        final BSONObject selector = delete.getSelector();
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
        final BSONObject selector = updateCommand.getSelector();
        final BSONObject update = updateCommand.getUpdate();
        final boolean multi = updateCommand.isMulti();
        final boolean upsert = updateCommand.isUpsert();

        try {
            BSONObject result = updateDocuments(channel, collectionName, selector, update, multi, upsert);
            putLastResult(channel, result);
        } catch (MongoServerException e) {
            log.error("failed to update {}", updateCommand, e);
        }
    }

    protected void addIndex(BSONObject indexDescription) throws MongoServerException {
        openOrCreateIndex(indexDescription);
        getOrCreateIndexesCollection().addDocument(indexDescription);
    }

    private MongoCollection<KEY> getOrCreateIndexesCollection() throws MongoServerException {
        synchronized(indexes) {
            if (indexes.get() == null) {
                MongoCollection<KEY> indexCollection = openOrCreateCollection(INDEXES_COLLECTION_NAME, null);
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

    private void openOrCreateIndex(BSONObject indexDescription) throws MongoServerException {
        String ns = indexDescription.get("ns").toString();
        String collectionName = extractCollectionNameFromNamespace(ns);

        MongoCollection<KEY> collection = resolveOrCreateCollection(collectionName);

        BSONObject key = (BSONObject) indexDescription.get("key");
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

    protected abstract Index<KEY> openOrCreateUniqueIndex(String collectionName, String key, boolean ascending);

    protected BSONObject insertDocuments(final Channel channel, final String collectionName, final List<BSONObject> documents) throws MongoServerException {
        clearLastStatus(channel);
        try {
            if (collectionName.startsWith("system.")) {
                throw new MongoServerError(16459, "attempt to insert in system namespace");
            }
            final MongoCollection<KEY> collection = resolveOrCreateCollection(collectionName);
            int n = collection.insertDocuments(documents);
            assert n == documents.size();
            final BSONObject result = new BasicBSONObject("n", Integer.valueOf(n));
            putLastResult(channel, result);
            return result;
        } catch (MongoServerError e) {
            putLastError(channel, e);
            throw e;
        }
    }

    protected BSONObject deleteDocuments(final Channel channel, final String collectionName, final BSONObject selector, final int limit) throws MongoServerException {
        clearLastStatus(channel);
        try {
            if (collectionName.startsWith("system.")) {
                throw new MongoServerError(12050, "cannot delete from system namespace");
            }
            MongoCollection<KEY> collection = resolveCollection(collectionName, false);
            int n;
            if (collection == null) {
                n = 0;
            } else {
                n = collection.deleteDocuments(selector, limit);
            }
            final BSONObject result = new BasicBSONObject("n", Integer.valueOf(n));
            putLastResult(channel, result);
            return result;
        } catch (MongoServerError e) {
            putLastError(channel, e);
            throw e;
        }
    }

    protected BSONObject updateDocuments(final Channel channel, final String collectionName, final BSONObject selector, final BSONObject update, final boolean multi, final boolean upsert)
            throws MongoServerException {
                clearLastStatus(channel);
                try {
                    if (collectionName.startsWith("system.")) {
                        throw new MongoServerError(10156, "cannot update system collection");
                    }

                    MongoCollection<KEY> collection = resolveOrCreateCollection(collectionName);
                    return collection.updateDocuments(selector, update, multi, upsert);
                } catch (MongoServerException e) {
                    putLastError(channel, e);
                    throw e;
                }
            }

    protected void putLastError(Channel channel, MongoServerException ex) {
        BSONObject error = new BasicBSONObject();
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

    protected synchronized void putLastResult(Channel channel, BSONObject result) {
        List<BSONObject> results = lastResults.get(channel);
        // list must not be empty
        BSONObject last = results.get(results.size() - 1);
        if (last != null) {
            throw new IllegalStateException("last result already set: " + last);
        }
        results.set(results.size() - 1, result);
    }

    protected MongoCollection<KEY> createCollection(String collectionName) throws MongoServerException {
        checkCollectionName(collectionName);
        if (collectionName.contains("$")) {
            throw new MongoServerError(10093, "cannot insert into reserved $ collection");
        }

        MongoCollection<KEY> collection = openOrCreateCollection(collectionName, Constants.ID_FIELD);
        addNamespace(collection);

        BSONObject indexDescription = new BasicBSONObject();
        indexDescription.put("name", "_id_");
        indexDescription.put("ns", collection.getFullName());
        indexDescription.put("key", new BasicBSONObject("_id", Integer.valueOf(1)));
        addIndex(indexDescription);

        log.info("created collection {}", collection.getFullName());

        return collection;
    }

    protected abstract MongoCollection<KEY> openOrCreateCollection(String collectionName, String idField);

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
    public MongoCollection<KEY> deregisterCollection(String collectionName) throws MongoServerException {
        MongoCollection<KEY> removedCollection = collections.remove(collectionName);
        namespaces.deleteDocuments(new BasicBSONObject("name", removedCollection.getFullName()), 1);
        return removedCollection;
    }

    @Override
    public void moveCollection(MongoDatabase oldDatabase, MongoCollection<?> collection, String newCollectionName)
            throws MongoServerException {
        oldDatabase.deregisterCollection(collection.getCollectionName());
        collection.renameTo(getDatabaseName(), newCollectionName);
        // TODO resolve cast
        @SuppressWarnings("unchecked")
        MongoCollection<KEY> newCollection = (MongoCollection<KEY>) collection;
        collections.put(newCollectionName, newCollection);
        List<BSONObject> newDocuments = new ArrayList<BSONObject>();
        newDocuments.add(new BasicBSONObject("name", collection.getFullName()));
        namespaces.insertDocuments(newDocuments);
    }

}
