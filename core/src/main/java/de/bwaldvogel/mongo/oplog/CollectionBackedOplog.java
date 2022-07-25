package de.bwaldvogel.mongo.oplog;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.backend.CursorRegistry;
import de.bwaldvogel.mongo.backend.TailableCursor;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.backend.aggregation.Aggregation;
import de.bwaldvogel.mongo.bson.BsonTimestamp;
import de.bwaldvogel.mongo.bson.Document;

public class CollectionBackedOplog implements Oplog {

    private static final long ELECTION_TERM = 1L;
    private static final String START_AT_OPERATION_TIME = "startAtOperationTime";
    private static final String START_AFTER = "startAfter";
    private static final String RESUME_AFTER = "resumeAfter";

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
        Stream<Document> oplogInsertDocuments = documents.stream()
            .map(document -> toOplogInsertDocument(namespace, document));
        addDocuments(oplogInsertDocuments);
    }

    @Override
    public void handleUpdate(String namespace, Document selector, Document query, List<Object> modifiedIds) {
        if (isOplogCollection(namespace)) {
            return;
        }
        Stream<Document> oplogUpdateDocuments = modifiedIds.stream()
            .map(id -> toOplogUpdateDocument(namespace, query, id));
        addDocuments(oplogUpdateDocuments);
    }

    @Override
    public void handleDelete(String namespace, Document query, List<Object> deletedIds) {
        if (isOplogCollection(namespace)) {
            return;
        }
        Stream<Document> oplogDeleteDocuments = deletedIds.stream()
            .map(id -> toOplogDeleteDocument(namespace, id));
        addDocuments(oplogDeleteDocuments);
    }

    private void addDocuments(Stream<Document> oplogDocuments) {
        collection.addDocuments(oplogDocuments);
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

    private Stream<Document> streamOplog(OplogPosition position, String namespace) {
        return collection.queryAllAsStream()
            .filter(document -> filterNamespace(document, namespace))
            .filter(document -> {
                BsonTimestamp timestamp = OplogUtils.getOplogTimestamp(document);
                OplogPosition documentOplogPosition = new OplogPosition(timestamp);
                return documentOplogPosition.isAfter(position);
            })
            .sorted((o1, o2) -> {
                BsonTimestamp timestamp1 = OplogUtils.getOplogTimestamp(o1);
                BsonTimestamp timestamp2 = OplogUtils.getOplogTimestamp(o2);
                return timestamp1.compareTo(timestamp2);
            });
    }

    private static boolean filterNamespace(Document document, String namespace) {
        String docNS = (String) document.get(OplogDocumentFields.NAMESPACE);
        if (docNS.equals(namespace)) {
            return true;
        }
        return Utils.getDatabaseNameFromFullName(namespace).equals(Utils.getDatabaseNameFromFullName(docNS))
            && Utils.getCollectionNameFromFullName(docNS).equals("$cmd");
    }

    @Override
    public OplogCursor createCursor(String namespace, OplogPosition initialOplogPosition) {
        return new OplogCursor(
            cursorRegistry.generateCursorId(),
            position -> streamOplog(position, namespace),
            initialOplogPosition
        );
    }

    @Override
    public TailableCursor createChangeStreamCursor(Document changeStreamDocument, String namespace, Aggregation aggregation) {
        Document startAfter = (Document) changeStreamDocument.get(START_AFTER);
        Document resumeAfter = (Document) changeStreamDocument.get(RESUME_AFTER);
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
                    BsonTimestamp timestamp = OplogUtils.getOplogTimestamp(document);
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

        OplogCursor oplogCursor = createCursor(namespace, initialOplogPosition);
        ChangeStreamCursor cursor
            = new ChangeStreamCursor(backend, changeStreamDocument, aggregation, oplogCursor);
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

}
