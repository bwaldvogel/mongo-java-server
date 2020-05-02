package de.bwaldvogel.mongo;

import java.util.List;
import java.util.Spliterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import de.bwaldvogel.mongo.backend.ArrayFilters;
import de.bwaldvogel.mongo.backend.Index;
import de.bwaldvogel.mongo.backend.QueryResult;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.exception.InsertDocumentError;
import de.bwaldvogel.mongo.oplog.NoopOplog;
import de.bwaldvogel.mongo.oplog.Oplog;
import de.bwaldvogel.mongo.wire.message.MongoKillCursors;

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

    default QueryResult handleQuery(Document query, int numberToSkip, int numberToReturn) {
        return handleQuery(query, numberToSkip, numberToReturn, null);
    }

    QueryResult handleQuery(Document query, int numberToSkip, int numberToReturn, Document returnFieldSelector);

    QueryResult handleGetMore(long cursorId, int numberToReturn);

    void handleKillCursors(MongoKillCursors killCursors);

    default void insertDocuments(List<Document> documents) {
        int index = 0;
        for (Document document : documents) {
            try {
                addDocument(document);
                index++;
            } catch (MongoServerError e) {
                throw new InsertDocumentError(e, index);
            }
        }
    }

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
