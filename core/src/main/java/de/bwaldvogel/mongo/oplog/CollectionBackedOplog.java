package de.bwaldvogel.mongo.oplog;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.backend.Cursor;
import de.bwaldvogel.mongo.backend.CursorFactory;
import de.bwaldvogel.mongo.backend.MongoBackendClock;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.backend.aggregation.Aggregation;
import de.bwaldvogel.mongo.bson.BsonTimestamp;
import de.bwaldvogel.mongo.bson.Document;
import sun.nio.ch.Util;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CollectionBackedOplog implements Oplog {

    private static final long ELECTION_TERM = 1L;
    private final String START_AT_OPERATION_TIME = "startAtOperationTime";

    private final MongoBackendClock clock;
    private final MongoCollection<Document> collection;
    private MongoBackend backend;
    private CursorFactory cursorFactory;
    private final UUID ui = UUID.randomUUID();

    public CollectionBackedOplog(MongoBackend backend, MongoCollection<Document> collection, CursorFactory cursorFactory) {
        this.clock = backend.getClock();
        this.collection = collection;
        this.backend = backend;
        this.cursorFactory = cursorFactory;
    }

    @Override
    public void handleInsert(String namespace, List<Document> documents) {
        if (isOplogCollection(namespace)) {
            return;
        }
        documents.stream()
            .map(document -> toOplogInsertDocument(namespace, document))
            .forEach(collection::addDocument);
    }

    @Override
    public void handleUpdate(String namespace, Document selector, Document query, List<Object> modifiedIds) {
        if (isOplogCollection(namespace)) {
            return;
        }
        modifiedIds.forEach(id ->
            collection.addDocument(toOplogUpdateDocument(namespace, query, id))
        );
    }

    @Override
    public void handleDelete(String namespace, Document query, List<Object> deletedIds) {
        if (isOplogCollection(namespace)) {
            return;
        }
        deletedIds.forEach(id ->
            collection.addDocument(toOplogDeleteDocument(namespace, id))
        );

    }

    @Override
    public Stream<Document> handleAggregation(Document changeStreamDocument) {
        final long clientToken = Utils.getResumeTokenFromChangeStreamDocument(changeStreamDocument);
        final BsonTimestamp startAtOperationTime = changeStreamDocument.containsKey(START_AT_OPERATION_TIME) ?
            (BsonTimestamp) changeStreamDocument.get(START_AT_OPERATION_TIME) : null;

        List<Document> res = collection.queryAllAsStream()
            .filter(document -> {
                if (startAtOperationTime != null) {
                    BsonTimestamp serverTs = (BsonTimestamp) document.get("ts");
                    return serverTs.compareTo(startAtOperationTime) >= 0;
                }
                return Utils.getResumeToken((BsonTimestamp) document.get("ts")) > clientToken;
            })
            .sorted((o1, o2) -> {
                BsonTimestamp timestamp1 = (BsonTimestamp) o1.get("ts");
                BsonTimestamp timestamp2 = (BsonTimestamp) o2.get("ts");
                return timestamp1.compareTo(timestamp2);
            })
            .map(document -> getChangeStreamResponseDocument(document, changeStreamDocument))
            .collect(Collectors.toList());
        return res.stream();
    }

    @Override
    public Cursor createCursor(Aggregation aggregation) {
        return cursorFactory.createTailableCursor(aggregation, clock);
    }

    @Override
    public Cursor createCursor(Aggregation aggregation, long resumeToken) {
        return cursorFactory.createTailableCursor(aggregation, resumeToken);
    }

    private Document toOplogDocument(OperationType operationType, String namespace) {
        return new Document()
            .append("ts", clock.increaseAndGet())
            .append("t", ELECTION_TERM)
            .append("h", 0L)
            .append("v", 2L)
            .append("op", operationType.getCode())
            .append("ns", namespace)
            .append("ui", ui)
            .append("wall", clock.instant());
    }

    private Document toOplogInsertDocument(String namespace, Document document) {
        return toOplogDocument(OperationType.INSERT, namespace)
            .append("o", document.cloneDeeply());
    }

    private Document toOplogUpdateDocument(String namespace, Document query, Object id) {
        return toOplogDocument(OperationType.UPDATE, namespace)
            .append("o", query)
            .append("o2", new Document("_id", id));
    }

    private Document toOplogDeleteDocument(String namespace, Object deletedDocumentId) {
        return toOplogDocument(OperationType.DELETE, namespace)
            .append("o", new Document("_id", deletedDocumentId));
    }

    private boolean isOplogCollection(String namespace) {
        return collection.getFullName().equals(namespace);
    }

    private Document getFullDocument(Document changeStreamDocument, Document document, OperationType operationType) {
        switch (operationType) {
            case INSERT:
                return (Document) document.get("o");
            case DELETE:
                return null;
            case UPDATE:
                return lookUpUpdateDocument(changeStreamDocument, document);
        }
        throw new IllegalArgumentException("Invalid operation type");
    }

    private Document lookUpUpdateDocument(Document changeStreamDocument, Document document) {
        if (changeStreamDocument.containsKey("fullDocument") && changeStreamDocument.get("fullDocument").equals("updateLookup")) {
            String namespace = (String)document.get("ns");
            String databaseName = namespace.split("\\.")[0];
            String collectionName = namespace.split("\\.")[1];
            return backend.resolveDatabase(databaseName)
                .resolveCollection(collectionName, true)
                .queryAllAsStream().filter(d -> d.get("_id").equals(((Document)document.get("o2")).get("_id")))
                .findFirst().orElse(getDeltaUpdate((Document) document.get("o")));
        }
        return getDeltaUpdate((Document) document.get("o"));
    }

    private Document getDeltaUpdate(Document updateDocument) {
        Document delta = new Document();
        if (updateDocument.containsKey("$set")) {
            delta.appendAll((Document)updateDocument.get("$set"));
        }
        if (updateDocument.containsKey("$unset")) {
            delta.appendAll((Document)updateDocument.get("$unset"));
        }
        return delta;
    }

    private Document getChangeStreamResponseDocument(Document oplogDocument, Document changeStreamDocument) {
        OperationType operationType = OperationType.fromCode(oplogDocument.get("op").toString());
        Document documentKey = new Document();
        switch (operationType) {
            case UPDATE:
                documentKey = (Document)oplogDocument.get("o2");
                break;
            case INSERT:
                documentKey.append("_id", ((Document)oplogDocument.get("o")).get("_id"));
                break;
            case DELETE:
                documentKey = (Document)oplogDocument.get("o");
                break;
        }
        return new Document()
            .append("_id", new Document("_data", Utils.getResumeTokenAsHEX((BsonTimestamp) oplogDocument.get("ts")))) //This is going to be the resume token
            .append("operationType", operationType.getDescription())
            .append("fullDocument", getFullDocument(changeStreamDocument, oplogDocument, operationType))
            .append("documentKey", documentKey)
            .append("clusterTime", oplogDocument.get("ts"));
    }

}
