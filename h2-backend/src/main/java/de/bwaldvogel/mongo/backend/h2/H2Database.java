package de.bwaldvogel.mongo.backend.h2;

import org.bson.BSONObject;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.backend.AbstractMongoDatabase;
import de.bwaldvogel.mongo.backend.Index;
import de.bwaldvogel.mongo.exception.MongoServerException;

public class H2Database extends AbstractMongoDatabase<Object> {

    private MVStore mvStore;

    public H2Database(String databaseName, MongoBackend backend, MVStore mvStore) throws MongoServerException {
        super(databaseName, backend);
        this.mvStore = mvStore;
        initializeNamespacesAndIndexes();
    }

    @Override
    protected Index<Object> openOrCreateUniqueIndex(String collectionName, String key, boolean ascending) {
        MVMap<Object, Object> mvMap = mvStore.openMap(databaseName + "." + collectionName + "._index_" + key);
        return new H2UniqueIndex(key, ascending, mvMap);
    }

    @Override
    protected MongoCollection<Object> openOrCreateCollection(String collectionName, String idField) {
        MVMap<Object, BSONObject> mvMap = mvStore.openMap(databaseName + "." + collectionName);
        return new H2Collection(databaseName, collectionName, idField, mvMap);
    }

}
