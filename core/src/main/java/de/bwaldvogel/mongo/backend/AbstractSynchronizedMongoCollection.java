package de.bwaldvogel.mongo.backend;

import java.util.List;

import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.oplog.Oplog;
import de.bwaldvogel.mongo.wire.message.MongoKillCursors;

public abstract class AbstractSynchronizedMongoCollection<P> extends AbstractMongoCollection<P> {

    protected AbstractSynchronizedMongoCollection(MongoDatabase database, String collectionName, CollectionOptions options) {
        super(database, collectionName, options);
    }

    @Override
    public synchronized void addDocument(Document document) {
        super.addDocument(document);
    }

    @Override
    public synchronized Document findAndModify(Document query) {
        return super.findAndModify(query);
    }

    @Override
    public synchronized QueryResult handleQuery(Document queryObject, int numberToSkip, int numberToReturn,
                                                Document fieldSelector) {
        return super.handleQuery(queryObject, numberToSkip, numberToReturn, fieldSelector);
    }

    @Override
    public synchronized QueryResult handleGetMore(long cursorId, int numberToReturn) {
        return super.handleGetMore(cursorId, numberToReturn);
    }

    @Override
    public synchronized void handleKillCursors(MongoKillCursors killCursors) {
        super.handleKillCursors(killCursors);
    }

    @Override
    public synchronized Document handleDistinct(Document query) {
        return super.handleDistinct(query);
    }

    @Override
    public synchronized void insertDocuments(List<Document> documents) {
        super.insertDocuments(documents);
    }

    @Override
    public synchronized int deleteDocuments(Document selector, int limit, Oplog oplog) {
        return super.deleteDocuments(selector, limit, oplog);
    }

    @Override
    public synchronized Document updateDocuments(Document selector, Document updateQuery, ArrayFilters arrayFilters,
                                                 boolean isMulti, boolean isUpsert, Oplog oplog) {
        return super.updateDocuments(selector, updateQuery, arrayFilters, isMulti, isUpsert, oplog);
    }

    @Override
    public synchronized void removeDocument(Document document) {
        super.removeDocument(document);
    }

    @Override
    public synchronized void renameTo(MongoDatabase newDatabase, String newCollectionName) {
        super.renameTo(newDatabase, newCollectionName);
    }

}
