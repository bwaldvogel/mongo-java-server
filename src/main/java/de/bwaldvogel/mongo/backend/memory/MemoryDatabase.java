package de.bwaldvogel.mongo.backend.memory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.backend.AbstractMongoDatabase;
import de.bwaldvogel.mongo.backend.MongoBackend;
import de.bwaldvogel.mongo.backend.memory.index.UniqueIndex;
import de.bwaldvogel.mongo.exception.MongoServerException;

public class MemoryDatabase extends AbstractMongoDatabase<IntegerPosition> {

    public static final Logger log = LoggerFactory.getLogger(MemoryDatabase.class);

    public MemoryDatabase(MongoBackend backend, String databaseName) throws MongoServerException {
        super(databaseName, backend);
    }

    @Override
    public MemoryCollection createNewCollection(String collectionName) {
        return new MemoryCollection(getDatabaseName(), collectionName, "_id");
    }

    @Override
    protected UniqueIndex createUniqueIndex(String key, boolean ascending) {
        return new UniqueIndex(key, ascending);
    }

    @Override
    protected MemoryNamespacesCollection createNamespacesCollection(String databaseName) {
        return new MemoryNamespacesCollection(databaseName);
    }

    @Override
    protected MemoryIndexesCollection createIndexesCollection(String databaseName) {
        return new MemoryIndexesCollection(databaseName);
    }
}
