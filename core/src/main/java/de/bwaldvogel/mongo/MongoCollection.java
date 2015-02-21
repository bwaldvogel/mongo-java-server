package de.bwaldvogel.mongo;

import java.util.List;

import org.bson.BSONObject;

import de.bwaldvogel.mongo.backend.Index;
import de.bwaldvogel.mongo.exception.MongoServerException;

public interface MongoCollection<KEY> {

    String getFullName();

    String getCollectionName();

    void addIndex(Index<KEY> index);

    void addDocument(BSONObject document) throws MongoServerException;

    void removeDocument(BSONObject document) throws MongoServerException;

    Iterable<BSONObject> handleQuery(BSONObject query, int numberToSkip, int numberToReturn,
            BSONObject returnFieldSelector) throws MongoServerException;

    int insertDocuments(List<BSONObject> documents) throws MongoServerException;

    BSONObject updateDocuments(BSONObject selector, BSONObject update, boolean isMulti, boolean isUpsert)
            throws MongoServerException;

    int deleteDocuments(BSONObject selector, int limit) throws MongoServerException;

    BSONObject handleDistinct(BSONObject query) throws MongoServerException;

    BSONObject getStats() throws MongoServerException;

    BSONObject validate() throws MongoServerException;

    BSONObject findAndModify(BSONObject query) throws MongoServerException;

    int count(BSONObject query) throws MongoServerException;

    int count();

    int getNumIndexes();

    void drop();

}
