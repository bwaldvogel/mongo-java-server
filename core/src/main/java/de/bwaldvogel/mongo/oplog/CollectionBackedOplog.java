package de.bwaldvogel.mongo.oplog;

import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.backend.Cursor;
import de.bwaldvogel.mongo.backend.CursorRegistry;
import de.bwaldvogel.mongo.bson.BsonTimestamp;
import de.bwaldvogel.mongo.bson.Document;

public class CollectionBackedOplog implements Oplog {

    private static final long ELECTION_TERM = 1L;
    private static final String START_AT_OPERATION_TIME = "startAtOperationTime";

    private final OplogClock oplogClock;
    private final MongoCollection<Document> collection;
    private final MongoBackend backend;
    private final CursorRegistry cursorRegistry;
    private final UUID ui = UUID.randomUUID();

    public CollectionBackedOplog(MongoBackend backend, MongoCollection<Document> collection, CursorRegistry cursorRegistry) {
        this.oplogClock = new OplogClock(backend.getClock());
        this.collection = collection;
        this.backend = backend;
        this.cursorRegistry = cursorRegistry;
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

    private Stream<Document> streamOplog(Document changeStreamDocument, OplogPosition position) {
        return collection.queryAllAsStream()
            .filter(document -> {
                BsonTimestamp timestamp = (BsonTimestamp) document.get("ts");
                OplogPosition documentOplogPosition = new OplogPosition(timestamp);
                return documentOplogPosition.isAfter(position);
            })
            .sorted((o1, o2) -> {
                BsonTimestamp timestamp1 = (BsonTimestamp) o1.get("ts");
                BsonTimestamp timestamp2 = (BsonTimestamp) o2.get("ts");
                return timestamp1.compareTo(timestamp2);
            })
            .map(document -> toChangeStreamResponseDocument(document, changeStreamDocument));
    }

    @Override
    public Cursor createCursor(Document changeStreamDocument) {
        Document startAfter = (Document) changeStreamDocument.get("startAfter");
        Document resumeAfter = (Document) changeStreamDocument.get("resumeAfter");
        BsonTimestamp startAtOperationTime = (BsonTimestamp) changeStreamDocument.get(START_AT_OPERATION_TIME);
        final OplogPosition initialOplogPosition;
        if (startAfter != null) {
            initialOplogPosition = OplogPosition.fromDocument(startAfter);
        } else if (resumeAfter != null) {
            initialOplogPosition = OplogPosition.fromDocument(resumeAfter);
        } else if (startAtOperationTime != null) {
            initialOplogPosition = new OplogPosition(startAtOperationTime).inclusive();
        } else {
            initialOplogPosition = new OplogPosition(oplogClock.now());
        }

        Function<OplogPosition, Stream<Document>> streamSupplier = position -> streamOplog(changeStreamDocument, position);
        Cursor cursor = new OplogCursor(cursorRegistry.generateCursorId(), streamSupplier, initialOplogPosition);
        cursorRegistry.add(cursor);
        return cursor;
    }

    private Document toOplogDocument(OperationType operationType, String namespace) {
        return new Document()
            .append("ts", oplogClock.incrementAndGet())
            .append("t", ELECTION_TERM)
            .append("h", 0L)
            .append("v", 2L)
            .append("op", operationType.getCode())
            .append("ns", namespace)
            .append("ui", ui)
            .append("wall", oplogClock.instant());
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
            String namespace = (String) document.get("ns");
            String databaseName = namespace.split("\\.")[0];
            String collectionName = namespace.split("\\.")[1];
            return backend.resolveDatabase(databaseName)
                .resolveCollection(collectionName, true)
                .queryAllAsStream().filter(d -> d.get("_id").equals(((Document) document.get("o2")).get("_id")))
                .findFirst().orElse(getDeltaUpdate((Document) document.get("o")));
        }
        return getDeltaUpdate((Document) document.get("o"));
    }

    private Document getDeltaUpdate(Document updateDocument) {
        Document delta = new Document();
        if (updateDocument.containsKey("$set")) {
            delta.appendAll((Document) updateDocument.get("$set"));
        }
        if (updateDocument.containsKey("$unset")) {
            delta.appendAll((Document) updateDocument.get("$unset"));
        }
        return delta;
    }

    private Document toChangeStreamResponseDocument(Document oplogDocument, Document changeStreamDocument) {
        OperationType operationType = OperationType.fromCode(oplogDocument.get("op").toString());
        Document documentKey = new Document();
        switch (operationType) {
            case UPDATE:
                documentKey = (Document) oplogDocument.get("o2");
                break;
            case INSERT:
                documentKey.append("_id", ((Document) oplogDocument.get("o")).get("_id"));
                break;
            case DELETE:
                documentKey = (Document) oplogDocument.get("o");
                break;
        }
        OplogPosition resumeToken = new OplogPosition((BsonTimestamp) oplogDocument.get("ts"));
        return new Document()
            .append("_id", new Document("_data", resumeToken.toHexString()))
            .append("operationType", operationType.getDescription())
            .append("fullDocument", getFullDocument(changeStreamDocument, oplogDocument, operationType))
            .append("documentKey", documentKey)
            .append("clusterTime", oplogDocument.get("ts"));
    }

}
