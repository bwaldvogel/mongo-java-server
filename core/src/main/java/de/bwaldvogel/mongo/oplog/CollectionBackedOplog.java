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
            .map(document -> toOplogDocument(OperationType.INSERT, namespace, document))
            .forEach(collection::addDocument);
    }

    @Override
    public void handleUpdate(String namespace, Document selector, Document query) {
        if (isOplogCollection(namespace)) {
            return;
        }
        collection.addDocument(toOplogDocument(OperationType.UPDATE, namespace, selector).append("o2", query));
    }

    @Override
    public void handleDelete(String namespace, Document query) {
        if (isOplogCollection(namespace)) {
            return;
        }
        collection.addDocument(toOplogDocument(OperationType.DELETE, namespace, query));
    }

    private Document toOplogDocument(OperationType operationType, String namespace, Document document) {
        Instant now = clock.instant();
        return new Document()
            .append("ts", new BsonTimestamp(now))
            .append("t", ELECTION_TERM)
            .append("h", 0L)
            .append("v", 2L)
            .append("op", operationType.getCode())
            .append("ns", namespace)
            .append("ui", ui)
            .append("wall", now)
            .append("o", document.cloneDeeply());
    }

    private boolean isOplogCollection(String namespace) {
        return collection.getFullName().equals(namespace);
    }

}
