package de.bwaldvogel.mongo.oplog;

import java.time.Instant;
import java.util.UUID;

import de.bwaldvogel.mongo.bson.Bson;
import de.bwaldvogel.mongo.bson.BsonTimestamp;
import de.bwaldvogel.mongo.bson.Document;

public class OplogDocument implements Bson {
    private Document document;

    public OplogDocument() {
        document = new Document();
        withProtocolVersion(2L);
    }

    public Document toDocument() {
        return document;
    }

    public BsonTimestamp getTimestamp() {
        return (BsonTimestamp) document.get("ts");
    }

    public OplogDocument withTimestamp(BsonTimestamp timestamp) {
        document.put("ts", timestamp);
        return this;
    }

    public long getT() {
        return (Long) document.get("t");
    }

    public OplogDocument withT(long t) {
        document.put("t", t);
        return this;
    }

    public long getHash() {
        return (Long) document.get("h");
    }

    public OplogDocument withHash(long h) {
        document.put("h", h);
        return this;
    }

    public long getProtocolVersion() {
        return (Long) document.get("v");
    }

    public OplogDocument withProtocolVersion(long protocolVersion) {
        document.put("v", protocolVersion);
        return this;
    }

    public OperationType getOperationType() {
        return OperationType.valueOf((String) document.get("op"));
    }

    public OplogDocument withOperationType(OperationType operationType) {
        document.put("op", operationType.getValue());
        return this;
    }

    public String getNamespace() {
        return (String) document.get("ns");
    }

    public OplogDocument withNamespace(String namespace) {
        document.put("ns", namespace);
        return this;
    }

    public UUID getUUID() {
        return (UUID) document.get("ui");
    }

    public OplogDocument withUUID(UUID uuid) {
        document.put("ui", uuid);
        return this;
    }

    public Instant getWall() {
        return (Instant) document.get("wall");
    }

    public OplogDocument withWall(Instant wall) {
        document.put("wall", wall);
        return this;
    }

    public Document getOperationDocument() {
        return (Document) document.get("o");
    }

    public OplogDocument withOperationDocument(Document operationDocument) {
        document.put("o", operationDocument);
        return this;
    }

    public Document getAdditionalOperationDocument() {
        return (Document) document.get("o2");
    }

    public OplogDocument withAdditionalOperationalDocument(Document operationalDocument) {
        document.put("o2", operationalDocument);
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (o == this) {
            return true;
        }
        if (!(o instanceof OplogDocument)) {
            return false;
        }
        return toDocument().equals(((OplogDocument)o).toDocument());
    }

    @Override
    public int hashCode() {
        return toDocument().hashCode();
    }
}
