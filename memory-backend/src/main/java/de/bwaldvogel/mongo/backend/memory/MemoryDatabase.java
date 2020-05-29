package de.bwaldvogel.mongo.backend.memory;

import java.util.List;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.backend.AbstractMongoDatabase;
import de.bwaldvogel.mongo.backend.CollectionOptions;
import de.bwaldvogel.mongo.backend.CursorFactory;
import de.bwaldvogel.mongo.backend.Index;
import de.bwaldvogel.mongo.backend.IndexKey;
import de.bwaldvogel.mongo.backend.memory.index.MemoryUniqueIndex;

public class MemoryDatabase extends AbstractMongoDatabase<Integer> {

    public MemoryDatabase(MongoBackend backend, String databaseName, CursorFactory cursorFactory) {
        super(databaseName, cursorFactory);
        initializeNamespacesAndIndexes();
    }

    @Override
    protected MemoryCollection openOrCreateCollection(String collectionName, CollectionOptions options) {
        return new MemoryCollection(this, collectionName, options, cursorFactory);
    }

    @Override
    protected Index<Integer> openOrCreateUniqueIndex(String collectionName, String indexName, List<IndexKey> keys, boolean sparse) {
        return new MemoryUniqueIndex(indexName, keys, sparse);
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
