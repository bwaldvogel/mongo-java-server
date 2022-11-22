package de.bwaldvogel.mongo;

import java.util.List;
import java.util.Spliterator;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import de.bwaldvogel.mongo.backend.ArrayFilters;
import de.bwaldvogel.mongo.backend.Index;
import de.bwaldvogel.mongo.backend.QueryParameters;
import de.bwaldvogel.mongo.backend.QueryResult;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.oplog.NoopOplog;
import de.bwaldvogel.mongo.oplog.Oplog;

public interface MongoCollection<P> {

    UUID getUuid();

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

    default void addDocuments(Stream<Document> documents) {
        documents.forEach(this::addDocument);
    }

    void removeDocument(Document document);

    default void addDocumentIfMissing(Document document) {
        if (!handleQuery(document, 0, 1).iterator().hasNext()) {
            addDocument(document);
        }
    }

    default Iterable<Document> queryAll() {
        return handleQuery(new Document());
    }

    default Stream<Document> queryAllAsStream() {
        Spliterator<Document> documents = queryAll().spliterator();
        return StreamSupport.stream(documents, false);
    }

    default Iterable<Document> handleQuery(Document query) {
        return handleQuery(query, 0, 0);
    }

    default Stream<Document> handleQueryAsStream(Document query) {
        Spliterator<Document> documents = handleQuery(query).spliterator();
        return StreamSupport.stream(documents, false);
    }

    default QueryResult handleQuery(Document query, int numberToSkip, int limit) {
        return handleQuery(new QueryParameters(query, numberToSkip, limit));
    }

    QueryResult handleQuery(QueryParameters queryParameters);

    default void insertDocuments(List<Document> documents) {
        insertDocuments(documents, true);
    }

    List<Document> insertDocuments(List<Document> documents, boolean isOrdered);

    Document updateDocuments(Document selector, Document update, ArrayFilters arrayFilters,
                             boolean isMulti, boolean isUpsert, Oplog oplog);

    default int deleteDocuments(Document selector, int limit) {
        return deleteDocuments(selector, limit, NoopOplog.get());
    }

    int deleteDocuments(Document selector, int limit, Oplog oplog);

    Document handleDistinct(Document query);

    Document getStats();

    Document validate();

    Document findAndModify(Document query);

    int count(Document query, int skip, int limit);

    default boolean isEmpty() {
        return count() == 0;
    }

    int count();

    default int getNumIndexes() {
        return getIndexes().size();
    }

    List<Index<P>> getIndexes();

    void renameTo(MongoDatabase newDatabase, String newCollectionName);

    void drop();

}
