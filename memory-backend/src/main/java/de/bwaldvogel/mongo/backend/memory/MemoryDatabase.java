package de.bwaldvogel.mongo.backend.memory;

import java.util.List;

import com.google.gson.Gson;

import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.AbstractSynchronizedMongoDatabase;
import de.bwaldvogel.mongo.backend.CollectionOptions;
import de.bwaldvogel.mongo.backend.CursorRegistry;
import de.bwaldvogel.mongo.backend.Index;
import de.bwaldvogel.mongo.backend.IndexKey;
import de.bwaldvogel.mongo.backend.memory.index.MemoryUniqueIndex;

public class MemoryDatabase extends AbstractSynchronizedMongoDatabase<Integer> {

    public MemoryDatabase(String databaseName, CursorRegistry cursorRegistry) {
        super(databaseName, cursorRegistry);
        initializeNamespacesAndIndexes();
    }

    @Override
    protected MemoryCollection openOrCreateCollection(String collectionName, CollectionOptions options) {
        return new MemoryCollection(this, collectionName, options, cursorRegistry);
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
