package de.bwaldvogel.mongo.oplog;

import java.time.Clock;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.backend.Cursor;
import de.bwaldvogel.mongo.backend.CursorRegistry;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.backend.aggregation.Aggregation;
import de.bwaldvogel.mongo.bson.BsonTimestamp;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerException;

public class CollectionBackedOplog implements Oplog {

    private static final long ELECTION_TERM = 1L;
    private static final String START_AT_OPERATION_TIME = "startAtOperationTime";
    private static final String FULL_DOCUMENT = "fullDocument";

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

    public CollectionBackedOplog(Clock clock, MongoCollection<Document> collection, CursorRegistry cursorRegistry) {
        this.oplogClock = new OplogClock(clock);
        this.collection = collection;
        this.cursorRegistry = cursorRegistry;
        this.backend = null;
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
    public void handleDropCollection(String namespace) {
        if (isOplogCollection(namespace)) {
            return;
        }
        final String databaseName = Utils.getDatabaseNameFromFullName(namespace);
        final String collectionName = Utils.getCollectionNameFromFullName(namespace);
        collection.addDocument(toOplogDropCollection(databaseName, collectionName));
    }

    private Stream<Document> streamOplog(Document changeStreamDocument, OplogPosition position, Aggregation aggregation) {
        return aggregation.runStagesAsStream(collection.queryAllAsStream()
            .filter(document -> {
                BsonTimestamp timestamp = getOplogTimestamp(document);
                OplogPosition documentOplogPosition = new OplogPosition(timestamp);
                return documentOplogPosition.isAfter(position);
            })
            .sorted((o1, o2) -> {
                BsonTimestamp timestamp1 = getOplogTimestamp(o1);
                BsonTimestamp timestamp2 = getOplogTimestamp(o2);
                return timestamp1.compareTo(timestamp2);
            })
            .map(document -> toChangeStreamResponseDocument(document, changeStreamDocument)));
    }

    @Override
    public Cursor createCursor(Document changeStreamDocument, String namespace, Aggregation aggregation) {
        Document startAfter = (Document) changeStreamDocument.get("startAfter");
        Document resumeAfter = (Document) changeStreamDocument.get("resumeAfter");
        BsonTimestamp startAtOperationTime = (BsonTimestamp) changeStreamDocument.get(START_AT_OPERATION_TIME);
        final OplogPosition initialOplogPosition;
        if (startAfter != null) {
            initialOplogPosition = OplogPosition.fromDocument(startAfter);
        } else if (resumeAfter != null) {
            initialOplogPosition = OplogPosition.fromDocument(resumeAfter);
            String databaseName = Utils.getDatabaseNameFromFullName(namespace);
            String collectionName = Utils.getCollectionNameFromFullName(namespace);
            boolean resumeAfterTerminalEvent = collection.queryAllAsStream()
                .filter(document -> {
                    BsonTimestamp timestamp = getOplogTimestamp(document);
                    OplogPosition documentOplogPosition = new OplogPosition(timestamp);
                    return initialOplogPosition.isAfter(documentOplogPosition.inclusive());
                })
                .anyMatch(document -> document.get(OplogDocumentFields.OPERATION_TYPE).equals(OperationType.COMMAND.getCode())
                    && document.get(OplogDocumentFields.NAMESPACE).equals(String.format("%s.$cmd", databaseName))
                    && document.get(OplogDocumentFields.O).equals(new Document("drop", collectionName))
                );

            if (resumeAfterTerminalEvent) {
                return new InvalidateOplogCursor(initialOplogPosition);
            }
        } else if (startAtOperationTime != null) {
            initialOplogPosition = new OplogPosition(startAtOperationTime).inclusive();
        } else {
            initialOplogPosition = new OplogPosition(oplogClock.now());
        }

        Function<OplogPosition, Stream<Document>> streamSupplier = position -> streamOplog(changeStreamDocument, position, aggregation);
        OplogCursor cursor = new OplogCursor(cursorRegistry.generateCursorId(), streamSupplier, initialOplogPosition);
        cursorRegistry.add(cursor);
        return cursor;
    }

    private Document toOplogDocument(OperationType operationType, String namespace) {
        return new Document()
            .append(OplogDocumentFields.TIMESTAMP, oplogClock.incrementAndGet())
            .append("t", ELECTION_TERM)
            .append("h", 0L)
            .append("v", 2L)
            .append("op", operationType.getCode())
            .append(OplogDocumentFields.NAMESPACE, namespace)
            .append("ui", ui)
            .append("wall", oplogClock.instant());
    }

    private Document toOplogInsertDocument(String namespace, Document document) {
        return toOplogDocument(OperationType.INSERT, namespace)
            .append(OplogDocumentFields.O, document.cloneDeeply());
    }

    private Document toOplogUpdateDocument(String namespace, Document query, Object id) {
        return toOplogDocument(OperationType.UPDATE, namespace)
            .append(OplogDocumentFields.O, query)
            .append(OplogDocumentFields.O2, new Document(OplogDocumentFields.ID, id));
    }

    private Document toOplogDeleteDocument(String namespace, Object deletedDocumentId) {
        return toOplogDocument(OperationType.DELETE, namespace)
            .append(OplogDocumentFields.O, new Document(OplogDocumentFields.ID, deletedDocumentId));
    }

    private Document toOplogDropCollection(String databaseName, String collectionName) {
        return toOplogDocument(OperationType.COMMAND, String.format("%s.$cmd", databaseName))
            .append(OplogDocumentFields.O, new Document("drop", collectionName));
    }

    private boolean isOplogCollection(String namespace) {
        return collection.getFullName().equals(namespace);
    }

    private Document getFullDocument(Document changeStreamDocument, Document document, OperationType operationType) {
        switch (operationType) {
            case INSERT:
                return getUpdateDocument(document);
            case DELETE:
                return null;
            case UPDATE:
                return lookUpUpdateDocument(changeStreamDocument, document);
        }
        throw new IllegalArgumentException("Invalid operation type");
    }

    private Document lookUpUpdateDocument(Document changeStreamDocument, Document document) {
        Document deltaUpdate = getDeltaUpdate(getUpdateDocument(document));
        if (changeStreamDocument.containsKey(FULL_DOCUMENT) && changeStreamDocument.get(FULL_DOCUMENT).equals("updateLookup")) {
            String namespace = (String) document.get(OplogDocumentFields.NAMESPACE);
            String databaseName = namespace.split("\\.")[0];
            String collectionName = namespace.split("\\.")[1];
            return backend.resolveDatabase(databaseName)
                .resolveCollection(collectionName, true)
                .queryAllAsStream()
                .filter(d -> d.get(OplogDocumentFields.ID).equals(((Document) document.get(OplogDocumentFields.O2)).get(OplogDocumentFields.ID)))
                .findFirst()
                .orElse(deltaUpdate);
        }
        return deltaUpdate;
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
        Document document = getUpdateDocument(oplogDocument);
        BsonTimestamp timestamp = getOplogTimestamp(oplogDocument);
        OplogPosition oplogPosition = new OplogPosition(timestamp);
        switch (operationType) {
            case UPDATE:
            case DELETE:
                documentKey = document;
                break;
            case INSERT:
                documentKey.append(OplogDocumentFields.ID, document.get(OplogDocumentFields.ID));
                break;
            case COMMAND:
                return toChangeStreamCommandResponseDocument(oplogDocument, oplogPosition, timestamp);
            default:
                throw new IllegalArgumentException("Unexpected operation type: " + operationType);
        }

        return new Document()
            .append(OplogDocumentFields.ID, new Document(OplogDocumentFields.ID_DATA_KEY, oplogPosition.toHexString()))
            .append("operationType", operationType.getDescription())
            .append(FULL_DOCUMENT, getFullDocument(changeStreamDocument, oplogDocument, operationType))
            .append("documentKey", documentKey)
            .append("clusterTime", timestamp);
    }

    private Document toChangeStreamCommandResponseDocument(Document oplogDocument, OplogPosition oplogPosition, BsonTimestamp timestamp) {
        Document document = getUpdateDocument(oplogDocument);
        String operationType = document.keySet().stream().findFirst().orElseThrow(
            () -> new MongoServerException("Unspecified command operation type")
        );

        return new Document()
            .append(OplogDocumentFields.ID, new Document(OplogDocumentFields.ID_DATA_KEY, oplogPosition.toHexString()))
            .append("operationType", operationType)
            .append("clusterTime", timestamp);
    }

    private static BsonTimestamp getOplogTimestamp(Document document) {
        return (BsonTimestamp) document.get(OplogDocumentFields.TIMESTAMP);
    }

    private static Document getUpdateDocument(Document document) {
        return (Document) document.get(OplogDocumentFields.O);
    }

}
