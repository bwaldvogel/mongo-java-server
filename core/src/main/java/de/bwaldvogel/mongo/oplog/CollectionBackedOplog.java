package de.bwaldvogel.mongo.oplog;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.bson.BsonTimestamp;
import de.bwaldvogel.mongo.bson.Document;

public class CollectionBackedOplog extends AbstractOplog {

    private final MongoCollection<Document> collection;
    public CollectionBackedOplog(Clock clock, MongoCollection<Document> collection) {
        super(clock);
        this.collection = collection;
    }

    @Override
    public void handleInsert(String databaseName, Document query) {
        Instant instant = clock.instant();
        List<Document> documents = (List<Document>) query.get("documents");
        List<Document> oplogDocuments = documents.stream().map(d ->
            new OplogDocument()
                .withTimestamp(new BsonTimestamp(instant.toEpochMilli()))
                .withWall(instant)
                .withOperationType(OperationType.INSERT)
                .withOperationDocument(d)
                .withNamespace(String.format("%s.%s", databaseName, query.get("insert")))
                .asDocument()).collect(Collectors.toList());
        collection.insertDocuments(oplogDocuments);
    }

    @Override
    public void handleUpdate(String databaseName, Document query) {
    }

    @Override
    public void handleDelete(String databaseName, Document query) {

    }
}
