package de.bwaldvogel.mongo.backend;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.stream.Collectors;

import de.bwaldvogel.mongo.OplogBackend;
import de.bwaldvogel.mongo.bson.BsonTimestamp;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.oplog.OperationType;
import de.bwaldvogel.mongo.oplog.OplogDocument;
import de.bwaldvogel.mongo.wire.message.MongoInsert;
import io.netty.channel.Channel;

public abstract class OplogMongoBackend extends AbstractMongoBackend implements OplogBackend {

    public Document handleCommand(Channel channel, String databaseName, String command, Document query) {
        Document res = super.handleCommand(channel, databaseName, command, query);
        handleOplog(channel, databaseName, command, query);
        return res;
    }

    public void handleOplog(Channel channel, String databaseName, String command, Document query) {
        switch (command) {
            case "insert":
                handleOplogInsert(channel, databaseName, query);
                break;
            case "update":
                handleOplogUpdate(channel, databaseName, query);
                break;
            case "delete":
                handleOplogDelete(channel, databaseName, query);
                break;
        }
    }

    private void handleOplogInsert(Channel channel, String databaseName, Document query) {
        LocalDateTime now = LocalDateTime.now(clock);
        List<Document> documents = (List<Document>) query.get("documents");
        List<Document> oplogDocuments = documents.stream().map(d ->
            new OplogDocument()
                .withTimestamp(new BsonTimestamp(now.toEpochSecond(ZoneOffset.UTC)))
                .withWall(now.toInstant(ZoneOffset.UTC))
                .withOperationType(OperationType.INSERT)
                .withOperationDocument(d)
                .withNamespace(String.format("%s.%s", databaseName, query.get("insert")))
                .toDocument()).collect(Collectors.toList());
        MongoInsert mongoInsert = new MongoInsert(channel, null, "local.oplog.rs",
            oplogDocuments
        );
        handleInsert(mongoInsert);
    }

    private void handleOplogUpdate(Channel channel, String databaseName, Document query) {
        // Todo
    }

    private void handleOplogDelete(Channel channel, String databaseName, Document query) {
        // Todo
    }

}
