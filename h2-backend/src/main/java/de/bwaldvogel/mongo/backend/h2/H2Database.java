package de.bwaldvogel.mongo.backend.h2;

import java.util.List;
import java.util.stream.Collectors;

import org.h2.mvstore.FileStore;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.AbstractSynchronizedMongoDatabase;
import de.bwaldvogel.mongo.backend.CollectionOptions;
import de.bwaldvogel.mongo.backend.CursorRegistry;
import de.bwaldvogel.mongo.backend.Index;
import de.bwaldvogel.mongo.backend.IndexKey;
import de.bwaldvogel.mongo.backend.KeyValue;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.oplog.Oplog;

public class H2Database extends AbstractSynchronizedMongoDatabase<Object> {

    private static final Logger log = LoggerFactory.getLogger(H2Database.class);

    private static final String META_PREFIX = "meta.";
    static final String DATABASES_PREFIX = "databases.";

    private final MVStore mvStore;

    public H2Database(String databaseName, MVStore mvStore, CursorRegistry cursorRegistry) {
        super(databaseName, cursorRegistry);
        this.mvStore = mvStore;
        initializeNamespacesAndIndexes();
    }

    @Override
    protected Index<Object> openOrCreateUniqueIndex(String collectionName, String indexName, List<IndexKey> keys, boolean sparse) {
        MVMap<KeyValue, Object> mvMap = mvStore.openMap(mapNameForIndex(collectionName, indexName));
        return new H2UniqueIndex(mvMap, indexName, keys, sparse);
    }

    @Override
    protected void dropIndex(MongoCollection<Object> collection, String indexName) {
        super.dropIndex(collection, indexName);
        String name = mapNameForIndex(collection.getCollectionName(), indexName);
        log.debug("Removing map '{}'", name);
        mvStore.removeMap(name);
    }

    private String mapNameForIndex(String collectionName, String indexName) {
        return getFullCollectionNamespace(collectionName) + "._index_" + indexName;
    }

    @Override
    public void drop(Oplog oplog) {
        super.drop(oplog);

        List<MVMap<?, ?>> maps = mvStore.getMapNames().stream()
            .filter(name -> name.startsWith(databaseName + ".")
                || name.startsWith(DATABASES_PREFIX + databaseName)
                || name.startsWith(META_PREFIX + databaseName)
            )
            .map(mvStore::openMap)
            .collect(Collectors.toList());

        for (MVMap<?, ?> map : maps) {
            mvStore.removeMap(map);
        }
    }

    @Override
    protected MongoCollection<Object> openOrCreateCollection(String collectionName, CollectionOptions options) {
        String fullCollectionName = getFullCollectionNamespace(collectionName);
        MVMap<Object, Document> dataMap = mvStore.openMap(DATABASES_PREFIX + fullCollectionName);
        MVMap<String, Object> metaMap = mvStore.openMap(META_PREFIX + fullCollectionName);
        return new H2Collection(this, collectionName, options, dataMap, metaMap, cursorRegistry);
    }

    @Override
    protected long getStorageSize() {
        FileStore<?> fileStore = mvStore.getFileStore();
        if (fileStore != null) {
            return fileStore.size();
        } else {
            return 0;
        }
    }

    @Override
    protected long getFileSize() {
        return getStorageSize();
    }

    @Override
    public void dropCollection(String collectionName, Oplog oplog) {
        super.dropCollection(collectionName, oplog);
        String fullCollectionName = getFullCollectionNamespace(collectionName);
        MVMap<Object, Document> dataMap = mvStore.openMap(DATABASES_PREFIX + fullCollectionName);
        MVMap<String, Object> metaMap = mvStore.openMap(META_PREFIX + fullCollectionName);
        mvStore.removeMap(dataMap);
        mvStore.removeMap(metaMap);
    }

    @Override
    public void moveCollection(MongoDatabase oldDatabase, MongoCollection<?> collection, String newCollectionName) {
        super.moveCollection(oldDatabase, collection, newCollectionName);
        String fullCollectionName = collection.getFullName();
        String newFullName = getFullCollectionNamespace(newCollectionName);
        MVMap<Object, Document> dataMap = mvStore.openMap(DATABASES_PREFIX + fullCollectionName);
        MVMap<String, Object> metaMap = mvStore.openMap(META_PREFIX + fullCollectionName);

        mvStore.renameMap(dataMap, DATABASES_PREFIX + newFullName);
        mvStore.renameMap(metaMap, META_PREFIX + newFullName);
    }

}
