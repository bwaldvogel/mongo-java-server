package de.bwaldvogel.mongo.backend.memory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.backend.AbstractMongoDatabase;
import de.bwaldvogel.mongo.backend.memory.index.MemoryUniqueIndex;
import de.bwaldvogel.mongo.exception.MongoServerException;

public class MemoryDatabase extends AbstractMongoDatabase<Integer> {

    public static final Logger log = LoggerFactory.getLogger(MemoryDatabase.class);

    public MemoryDatabase(MongoBackend backend, String databaseName) throws MongoServerException {
        super(databaseName, backend);
        initializeNamespacesAndIndexes();
    }

    @Override
    public MemoryCollection openOrCreateCollection(String collectionName, String idField) {
        return new MemoryCollection(getDatabaseName(), collectionName, idField);
    }

    @Override
    protected MemoryUniqueIndex openOrCreateUniqueIndex(String collectionName, String key, boolean ascending) {
        return new MemoryUniqueIndex(key, ascending);
    }

    @Override
    protected long getStorageSize() {
        return 0;
    }

    @Override
    protected long getFileSize() {
        return 0;
    }

}
