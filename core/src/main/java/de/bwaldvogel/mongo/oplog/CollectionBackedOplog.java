package de.bwaldvogel.mongo.oplog;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

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
        List<Document> oplogDocuments = documents.stream()
            .map(document -> toOplogDocument(document, namespace))
            .collect(Collectors.toList());
        collection.insertDocuments(oplogDocuments);
    }

    @Override
    public void handleUpdate(String databaseName, Document query) {
    }

    @Override
    public void handleDelete(String databaseName, Document query) {
    }

    private Document toOplogDocument(Document document, String namespace) {
        Instant now = clock.instant();
        return new Document()
            .append("ts", new BsonTimestamp(now))
            .append("t", ELECTION_TERM)
            .append("h", 0L)
            .append("v", 2L)
            .append("op", OperationType.INSERT.getCode())
            .append("ns", namespace)
            .append("ui", ui)
            .append("wall", now)
            .append("o", document);
    }

}
