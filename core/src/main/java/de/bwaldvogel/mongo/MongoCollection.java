package de.bwaldvogel.mongo;

import java.util.List;

import de.bwaldvogel.mongo.backend.Index;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerException;

public interface MongoCollection<P> {

    String getDatabaseName();

    String getFullName();

    String getCollectionName();

    void addIndex(Index<P> index);

    void addDocument(Document document) throws MongoServerException;

    void removeDocument(Document document) throws MongoServerException;

    Iterable<Document> handleQuery(Document query, int numberToSkip, int numberToReturn,
            Document returnFieldSelector) throws MongoServerException;

    int insertDocuments(List<Document> documents) throws MongoServerException;

    Document updateDocuments(Document selector, Document update, boolean isMulti, boolean isUpsert)
            throws MongoServerException;

    int deleteDocuments(Document selector, int limit) throws MongoServerException;

    Document handleDistinct(Document query) throws MongoServerException;

    Document getStats() throws MongoServerException;

    Document validate() throws MongoServerException;

    Document findAndModify(Document query) throws MongoServerException;

    int count(Document query, int skip, int limit) throws MongoServerException;

    int count();

    int getNumIndexes();

    void drop();

    void renameTo(String newDatabaseName, String newCollectionName);

}
