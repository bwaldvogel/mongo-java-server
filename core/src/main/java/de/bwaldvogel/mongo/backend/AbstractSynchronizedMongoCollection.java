package de.bwaldvogel.mongo.backend;

import java.util.List;

import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.oplog.Oplog;

public abstract class AbstractSynchronizedMongoCollection<P> extends AbstractMongoCollection<P> {

    protected AbstractSynchronizedMongoCollection(MongoDatabase database, String collectionName,
                                                  CollectionOptions options,
                                                  CursorRegistry cursorRegistry) {
        super(database, collectionName, options, cursorRegistry);
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
    public synchronized QueryResult handleQuery(QueryParameters queryParameters) {
        return super.handleQuery(queryParameters);
    }

    @Override
    public synchronized Document handleDistinct(Document query) {
        return super.handleDistinct(query);
    }

    @Override
    public synchronized List<Document> insertDocuments(List<Document> documents, boolean isOrdered) {
        return super.insertDocuments(documents, isOrdered);
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
