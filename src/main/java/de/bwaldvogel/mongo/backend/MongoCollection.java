package de.bwaldvogel.mongo.backend;

import java.util.List;

import org.bson.BSONObject;

import de.bwaldvogel.mongo.backend.memory.index.Index;
import de.bwaldvogel.mongo.exception.MongoServerException;

public abstract class MongoCollection {

    private String collectionName;
    private String databaseName;

    protected MongoCollection(String databaseName, String collectionName) {
        this.databaseName = databaseName;
        this.collectionName = collectionName;
    }

    public String getFullName() {
        return databaseName + "." + getCollectionName();
    }

    public String getCollectionName() {
        return collectionName;
    }

    public abstract void addIndex(Index index);

    public abstract void addDocument(BSONObject document) throws MongoServerException;

    public abstract void removeDocument(BSONObject document) throws MongoServerException;

    public abstract Iterable<BSONObject> handleQuery(BSONObject query, int numberToSkip, int numberToReturn,
            BSONObject returnFieldSelector) throws MongoServerException;

    public abstract int insertDocuments(List<BSONObject> documents) throws MongoServerException;

    public abstract BSONObject updateDocuments(BSONObject selector, BSONObject update, boolean isMulti,
            boolean isUpsert) throws MongoServerException;

    public abstract int deleteDocuments(BSONObject selector, int limit) throws MongoServerException;

    public abstract BSONObject handleDistinct(BSONObject query) throws MongoServerException;

    public abstract BSONObject getStats() throws MongoServerException;

    public abstract BSONObject validate() throws MongoServerException;

    public abstract BSONObject findAndModify(BSONObject query) throws MongoServerException;

    public abstract int count(BSONObject query) throws MongoServerException;

    public abstract int count();

    public abstract int getNumIndexes();

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" + getFullName() + ")";
    }

}
