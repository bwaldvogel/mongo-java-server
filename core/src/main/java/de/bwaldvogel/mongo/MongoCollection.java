package de.bwaldvogel.mongo;

import java.util.List;

import de.bwaldvogel.mongo.backend.ArrayFilters;
import de.bwaldvogel.mongo.backend.Index;
import de.bwaldvogel.mongo.bson.Document;

public interface MongoCollection<P> {

    MongoDatabase getDatabase();

    default String getDatabaseName() {
        return getDatabase().getDatabaseName();
    }

    default String getFullName() {
        return getDatabaseName() + "." + getCollectionName();
    }

    String getCollectionName();

    void addIndex(Index<P> index);

    void dropIndex(String indexName);

    void addDocument(Document document);

    void removeDocument(Document document);

    default Iterable<Document> queryAll() {
        return handleQuery(new Document());
    }

    default Iterable<Document> handleQuery(Document query) {
        return handleQuery(query, 0, 0);
    }

    default Iterable<Document> handleQuery(Document query, int numberToSkip, int numberToReturn) {
        return handleQuery(query, numberToSkip, numberToReturn, null);
    }

    Iterable<Document> handleQuery(Document query, int numberToSkip, int numberToReturn, Document returnFieldSelector);

    void insertDocuments(List<Document> documents);

    Document updateDocuments(Document selector, Document update, ArrayFilters arrayFilters,
                             boolean isMulti, boolean isUpsert);

    int deleteDocuments(Document selector, int limit);

    Document handleDistinct(Document query);

    Document getStats();

    Document validate();

    Document findAndModify(Document query);

    int count(Document query, int skip, int limit);

    int count();

    int getNumIndexes();

    void renameTo(MongoDatabase newDatabase, String newCollectionName);

    void drop();

}
