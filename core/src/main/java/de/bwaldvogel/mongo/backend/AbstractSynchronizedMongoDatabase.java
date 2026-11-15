package de.bwaldvogel.mongo.backend;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.bson.Document;

public abstract class AbstractSynchronizedMongoDatabase<P> extends AbstractMongoDatabase<P> {

    protected AbstractSynchronizedMongoDatabase(String databaseName, CursorRegistry cursorRegistry) {
        super(databaseName, cursorRegistry);
    }

    @Override
    protected synchronized MongoCollection<P> resolveOrCreateCollection(String collectionName) {
        return super.resolveOrCreateCollection(collectionName);
    }

    @Override
    public synchronized MongoCollection<P> resolveCollection(String collectionName, boolean throwIfNotFound) {
        return super.resolveCollection(collectionName, throwIfNotFound);
    }

    @Override
    protected synchronized void addIndex(Document indexDescription) {
        super.addIndex(indexDescription);
    }

    @Override
    protected MongoCollection<P> getOrCreateIndexesCollection() {
        synchronized (indexes) {
            return super.getOrCreateIndexesCollection();
        }
    }

    @Override
    protected int countIndexes() {
        synchronized (indexes) {
            return super.countIndexes();
        }
    }
}
