package de.bwaldvogel.mongo.oplog;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.bson.BsonTimestamp;
import de.bwaldvogel.mongo.bson.Document;

public class CollectionBackedOplog implements Oplog {

    private static final long ELECTION_TERM = 1L;

    private final Clock clock;
    private final MongoCollection<Document> collection;
    private final UUID ui = UUID.randomUUID();

    public CollectionBackedOplog(Clock clock, MongoCollection<Document> collection) {
        this.clock = clock;
        this.collection = collection;
    }

    @Override
    public void handleInsert(String namespace, List<Document> documents) {
        if (isOplogCollection(namespace)) {
            return;
        }
        documents.stream()
            .map(document -> toOplogInsertDocument(OperationType.INSERT, namespace, document))
            .forEach(collection::addDocument);
    }

    @Override
    public void handleUpdate(String namespace, Document selector, Document query, List<Object> modifiedIds) {
        if (isOplogCollection(namespace)) {
            return;
        }
        modifiedIds.forEach(id ->
            collection.addDocument(toOplogUpdateDocument(OperationType.UPDATE, namespace, id).append("o", query))
        );

    }

    @Override
    public void handleDelete(String namespace, Document query, List<Object> deletedIds) {
        if (isOplogCollection(namespace)) {
            return;
        }
        deletedIds.forEach(id ->
            collection.addDocument(toOplogDeleteDocument(OperationType.DELETE, namespace, id))
        );

    }

    private Document toOplogDocument(OperationType operationType, String namespace) {
        Instant now = clock.instant();
        return new Document()
            .append("ts", new BsonTimestamp(now))
            .append("t", ELECTION_TERM)
            .append("h", 0L)
            .append("v", 2L)
            .append("op", operationType.getCode())
            .append("ns", namespace)
            .append("ui", ui)
            .append("wall", now);
    }

    private Document toOplogInsertDocument(OperationType operationType, String namespace, Document document) {
        return toOplogDocument(operationType, namespace).append("o", document.cloneDeeply());
    }

    private Document toOplogUpdateDocument(OperationType operationType, String namespace, Object updatedDocumentId) {
        return toOplogDocument(operationType, namespace).append("o2", new Document("_id", updatedDocumentId));
    }

    private Document toOplogDeleteDocument(OperationType operationType, String namespace, Object updatedDocumentId) {
        return toOplogDocument(operationType, namespace).append("o", new Document("_id", updatedDocumentId));
    }

    private boolean isOplogCollection(String namespace) {
        return collection.getFullName().equals(namespace);
    }

}
