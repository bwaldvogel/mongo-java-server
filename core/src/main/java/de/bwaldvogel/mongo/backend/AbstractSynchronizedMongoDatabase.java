package de.bwaldvogel.mongo.backend;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.bson.Document;
import io.netty.channel.Channel;

public abstract class AbstractSynchronizedMongoDatabase<P> extends AbstractMongoDatabase<P> {

    protected AbstractSynchronizedMongoDatabase(String databaseName, CursorRegistry cursorRegistry) {
        super(databaseName, cursorRegistry);
    }

    protected AbstractSynchronizedMongoDatabase(AbstractSynchronizedMongoDatabase database) {
        super(database);
    }

    @Override
    protected synchronized MongoCollection<P> resolveOrCreateCollection(String collectionName) {
        return super.resolveOrCreateCollection(collectionName);
    }

    @Override
    protected synchronized void clearLastStatus(Channel channel) {
        super.clearLastStatus(channel);
    }

    @Override
    public synchronized MongoCollection<P> resolveCollection(String collectionName, boolean throwIfNotFound) {
        return super.resolveCollection(collectionName, throwIfNotFound);
    }

    @Override
    protected MongoCollection<P> getOrCreateIndexesCollection() {
        synchronized (indexes) {
            return super.getOrCreateIndexesCollection();
        }
    }

    @Override
    protected synchronized void putLastResult(Channel channel, Document result) {
        super.putLastResult(channel, result);
    }

    @Override
    protected int countIndexes() {
        synchronized (indexes) {
            return super.countIndexes();
        }
    }
}
