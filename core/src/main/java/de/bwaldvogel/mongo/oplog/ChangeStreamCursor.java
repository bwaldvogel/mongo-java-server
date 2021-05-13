package de.bwaldvogel.mongo.oplog;

import java.util.List;
import java.util.stream.Collectors;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.backend.TailableCursor;
import de.bwaldvogel.mongo.backend.aggregation.Aggregation;
import de.bwaldvogel.mongo.bson.BsonTimestamp;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerException;

public class ChangeStreamCursor implements TailableCursor {

    private static final String FULL_DOCUMENT = "fullDocument";
    private static final String OPERATION_TYPE = "operationType";
    private static final String CLUSTER_TIME = "clusterTime";
    private static final String DOCUMENT_KEY = "documentKey";

    private final MongoBackend mongoBackend;
    private final Document changeStreamDocument;
    private final Aggregation aggregation;
    private final OplogCursor oplogCursor;

    ChangeStreamCursor(
        MongoBackend mongoBackend,
        Document changeStreamDocument,
        Aggregation aggregation,
        OplogCursor oplogCursor
    ) {
        this.mongoBackend = mongoBackend;
        this.changeStreamDocument = changeStreamDocument;
        this.aggregation = aggregation;
        this.oplogCursor = oplogCursor;
    }

    @Override
    public long getId() {
        return oplogCursor.getId();
    }

    @Override
    public boolean isEmpty() {
        return oplogCursor.isEmpty();
    }

    @Override
    public List<Document> takeDocuments(int numberToReturn) {
        return aggregation.runStagesAsStream(
            oplogCursor.takeDocuments(numberToReturn).stream()
                .map(this::toChangeStreamResponseDocument)
        ).collect(Collectors.toList());
    }

    @Override
    public OplogPosition getPosition() {
        return oplogCursor.getPosition();
    }

    private Document toChangeStreamResponseDocument(Document oplogDocument) {
        OperationType operationType = OperationType.fromCode(oplogDocument.get("op").toString());
        Document documentKey = new Document();
        Document document = getUpdateDocument(oplogDocument);
        BsonTimestamp timestamp = OplogUtils.getOplogTimestamp(oplogDocument);
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
            .append(OPERATION_TYPE, operationType.getDescription())
            .append(FULL_DOCUMENT, getFullDocument(changeStreamDocument, oplogDocument, operationType))
            .append(DOCUMENT_KEY, documentKey)
            .append(CLUSTER_TIME, timestamp);
    }

    private Document toChangeStreamCommandResponseDocument(Document oplogDocument, OplogPosition oplogPosition, BsonTimestamp timestamp) {
        Document document = getUpdateDocument(oplogDocument);
        String operationType = document.keySet().stream().findFirst().orElseThrow(
            () -> new MongoServerException("Unspecified command operation type")
        );

        return new Document()
            .append(OplogDocumentFields.ID, new Document(OplogDocumentFields.ID_DATA_KEY, oplogPosition.toHexString()))
            .append(OPERATION_TYPE, operationType)
            .append(CLUSTER_TIME, timestamp);
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

    private Document getUpdateDocument(Document document) {
        return (Document) document.get(OplogDocumentFields.O);
    }

    private Document lookUpUpdateDocument(Document changeStreamDocument, Document document) {
        Document deltaUpdate = getDeltaUpdate(getUpdateDocument(document));
        if (changeStreamDocument.containsKey(FULL_DOCUMENT) && changeStreamDocument.get(FULL_DOCUMENT).equals("updateLookup")) {
            String namespace = (String) document.get(OplogDocumentFields.NAMESPACE);
            String databaseName = namespace.split("\\.")[0];
            String collectionName = namespace.split("\\.")[1];
            return mongoBackend.resolveDatabase(databaseName)
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

}
