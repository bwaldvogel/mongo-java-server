package de.bwaldvogel.mongo.backend.memory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.jboss.netty.channel.Channel;

import de.bwaldvogel.mongo.backend.Constants;
import de.bwaldvogel.mongo.exception.InvalidNSError;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.exception.MongoServerException;
import de.bwaldvogel.mongo.exception.NSNameTooLongError;
import de.bwaldvogel.mongo.exception.NoSuchCommandException;
import de.bwaldvogel.mongo.exception.ReservedCollectionNameError;
import de.bwaldvogel.mongo.wire.message.ClientRequest;
import de.bwaldvogel.mongo.wire.message.MongoDelete;
import de.bwaldvogel.mongo.wire.message.MongoInsert;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import de.bwaldvogel.mongo.wire.message.MongoUpdate;

public class MemoryDatabase extends CommonDatabase {

    private static final Logger log = Logger.getLogger(MemoryDatabase.class);

    private Map<String, MemoryCollection> collections = new HashMap<String, MemoryCollection>();
    private Map<Channel, MongoServerError> lastExceptions = new HashMap<Channel, MongoServerError>();
    private MemoryCollection namespaces;

    private MemoryBackend backend;

    public MemoryDatabase(MemoryBackend backend, String databaseName) {
        super(databaseName);
        this.backend = backend;
        namespaces = new MemoryCollection(getDatabaseName(), "system.namespaces", "name");
        collections.put("system.namespaces", namespaces);
    }

    private synchronized MemoryCollection resolveCollection(ClientRequest request) throws MongoServerException {
        return resolveCollection(request.getCollectionName());
    }

    private synchronized MemoryCollection resolveCollection(String collectionName) throws MongoServerException {
        MemoryCollection collection = collections.get(collectionName);
        if (collection == null) {
            checkCollectionName(collectionName);
            collection = new MemoryCollection(getDatabaseName(), collectionName, Constants.ID_FIELD);
            collections.put(collectionName, collection);
            namespaces.addDocument(new BasicBSONObject("name", collection.getFullName()));
        }
        return collection;
    }

    private void checkCollectionName(String collectionName) throws MongoServerException {

        if (collectionName.contains("$"))
            throw new ReservedCollectionNameError("illegal collection name: " + collectionName);

        if (collectionName.startsWith("system."))
            throw new ReservedCollectionNameError("illegal collection name: " + collectionName);

        if (collectionName.length() > Constants.MAX_NS_LENGTH)
            throw new NSNameTooLongError(Constants.MAX_NS_LENGTH);

        if (collectionName.isEmpty())
            throw new InvalidNSError(getDatabaseName() + "." + collectionName);
    }

    public MongoServerError getLastException(int clientId) {
        return lastExceptions.get(Integer.valueOf(clientId));
    }

    @Override
    public boolean isEmpty() {
        return collections.isEmpty();
    }

    @Override
    public Iterable<BSONObject> handleQuery(MongoQuery query) throws MongoServerException {
        MemoryCollection collection = resolveCollection(query);
        return collection.handleQuery(query.getQuery(), query.getNumberToSkip(), query.getNumberToReturn());
    }

    @Override
    public void handleClose(Channel channel) {
        lastExceptions.remove(channel);
    }

    @Override
    public void handleInsert(MongoInsert insert) throws MongoServerException {
        try {
            MemoryCollection collection = resolveCollection(insert);
            collection.handleInsert(insert);
        } catch (MongoServerError e) {
            log.error("failed to insert " + insert, e);
            lastExceptions.put(insert.getChannel(), e);
        }
    }

    @Override
    public void handleDelete(MongoDelete delete) throws MongoServerException {
        try {
            MemoryCollection collection = resolveCollection(delete);
            collection.handleDelete(delete);
        } catch (MongoServerError e) {
            log.error("failed to delete " + delete, e);
            lastExceptions.put(delete.getChannel(), e);
        }
    }

    @Override
    public void handleUpdate(MongoUpdate update) throws MongoServerException {
        try {
            MemoryCollection collection = resolveCollection(update);
            collection.handleUpdate(update);
        } catch (MongoServerError e) {
            log.error("failed to update " + update, e);
            lastExceptions.put(update.getChannel(), e);
        }
    }

    @Override
    public BSONObject handleCommand(Channel channel, String command, BSONObject query) throws MongoServerException {
        if (command.equals("count")) {
            return commandCount(command, query);
        } else if (command.equals("getlasterror")) {
            return commandGetLastError(channel, command, query);
        } else if (command.equals("drop")) {
            return commandDrop(query);
        } else if (command.equals("dropDatabase")) {
            return commandDropDatabase();
        } else if (command.equals("dbstats")) {
            return commandDatabaseStats();
        } else if (command.equals("collstats")) {
            String collectionName = query.get("collstats").toString();
            MemoryCollection collection = resolveCollection(collectionName);
            return collection.getStats();
        } else {
            log.error("unknown query: " + query);
        }
        throw new NoSuchCommandException(command);
    }

    private BSONObject commandDatabaseStats() {
        BSONObject response = new BasicBSONObject("db", getDatabaseName());
        response.put("collections", Integer.valueOf(collections.size()));

        int indexes = 0;
        long indexSize = 0;
        long objects = 0;
        long dataSize = 0;
        double averageObjectSize = 0;
        for (MemoryCollection collection : collections.values()) {
            objects += collection.getCount();
            dataSize += collection.getDataSize();
            indexes += collection.getNumIndexes();
            indexSize += collection.getIndexSize();
        }
        if (objects > 0) {
            averageObjectSize = dataSize / ((double) objects);
        }

        response.put("objects", Long.valueOf(objects));
        response.put("avgObjSize", Double.valueOf(averageObjectSize));
        response.put("dataSize", Long.valueOf(dataSize));
        response.put("storageSize", Long.valueOf(0));
        response.put("numExtents", Integer.valueOf(0));
        response.put("indexes", Integer.valueOf(indexes));
        response.put("indexSize", Long.valueOf(indexSize));
        response.put("fileSize", Integer.valueOf(0));
        response.put("nsSizeMB", Integer.valueOf(0));
        response.put("ok", Integer.valueOf(1));
        return response;
    }

    private BSONObject commandDropDatabase() {
        backend.dropDatabase(this);
        BSONObject response = new BasicBSONObject("dropped", getDatabaseName());
        response.put("ok", Integer.valueOf(1));
        return response;
    }

    private BSONObject commandDrop(BSONObject query) {
        String collectionName = query.get("drop").toString();
        MemoryCollection collection = collections.remove(collectionName);

        BSONObject response = new BasicBSONObject();
        if (collection == null) {
            response.put("errmsg", "ns not found");
            response.put("ok", Integer.valueOf(0));
        } else {
            namespaces.removeDocument(new BasicBSONObject("name", collection.getFullName()));
            response.put("nIndexesWas", Integer.valueOf(collection.getNumIndexes()));
            response.put("ns", collection.getFullName());
            response.put("ok", Integer.valueOf(1));
        }

        return response;
    }

    private BSONObject commandGetLastError(Channel channel, String command, BSONObject query) throws MongoServerError {
        Iterator<String> it = query.keySet().iterator();
        String cmd = it.next();
        if (!cmd.equals(command))
            throw new IllegalStateException();
        if (it.hasNext()) {
            String subCommand = it.next();
            if (!subCommand.equals("w")) {
                throw new IllegalArgumentException("unknown subcommand: " + subCommand);
            }
        }
        if (lastExceptions != null) {
            MongoServerError ex = lastExceptions.remove(channel);
            if (ex != null)
                throw ex;
        }
        return new BasicBSONObject("ok", Integer.valueOf(1));
    }

    private BSONObject commandCount(String command, BSONObject query) {
        String collection = query.get(command).toString();
        BSONObject response = new BasicBSONObject();
        MemoryCollection coll = collections.get(collection);
        if (coll == null) {
            response.put("missing", Boolean.TRUE);
            response.put("n", Integer.valueOf(0));
        } else {
            response.put("n", Integer.valueOf(coll.count((BSONObject) query.get("query"))));
        }
        response.put("ok", Integer.valueOf(1));
        return response;
    }
}
