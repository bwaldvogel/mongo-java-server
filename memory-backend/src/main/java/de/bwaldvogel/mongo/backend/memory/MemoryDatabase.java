package de.bwaldvogel.mongo.backend.memory;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.backend.AbstractMongoDatabase;
import de.bwaldvogel.mongo.backend.memory.index.MemoryUniqueIndex;

public class MemoryDatabase extends AbstractMongoDatabase<Integer> {

    public MemoryDatabase(MongoBackend backend, String databaseName) {
        super(databaseName, backend);
        initializeNamespacesAndIndexes();
    }

    @Override
    protected MemoryCollection openOrCreateCollection(String collectionName, String idField) {
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
