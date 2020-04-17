package de.bwaldvogel.mongo.backend;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.OplogBackend;
import de.bwaldvogel.mongo.bson.BsonTimestamp;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.oplog.OperationType;
import de.bwaldvogel.mongo.oplog.OplogDocument;
import de.bwaldvogel.mongo.wire.message.MongoDelete;
import de.bwaldvogel.mongo.wire.message.MongoGetMore;
import de.bwaldvogel.mongo.wire.message.MongoInsert;
import de.bwaldvogel.mongo.wire.message.MongoKillCursors;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import de.bwaldvogel.mongo.wire.message.MongoUpdate;
import io.netty.channel.Channel;

public final class OplogMongoBackend implements MongoBackend, OplogBackend {

    private MongoBackend delegate;

    public OplogMongoBackend(MongoBackend delegate) {
        this.delegate = delegate;
    }

    @Override
    public void handleClose(Channel channel) {
        delegate.handleClose(channel);
    }

    public Document handleCommand(Channel channel, String databaseName, String command, Document query) {
        Document res = delegate.handleCommand(channel, databaseName, command, query);
        handleOplog(channel, databaseName, command, query);
        return res;
    }

    @Override
    public QueryResult handleQuery(MongoQuery query) {
        return delegate.handleQuery(query);
    }

    @Override
    public QueryResult handleGetMore(MongoGetMore getMore) {
        return delegate.handleGetMore(getMore);
    }

    @Override
    public void handleInsert(MongoInsert insert) {
        delegate.handleInsert(insert);
    }

    @Override
    public void handleDelete(MongoDelete delete) {
        delegate.handleDelete(delete);
    }

    @Override
    public void handleUpdate(MongoUpdate update) {
        delegate.handleUpdate(update);
    }

    @Override
    public void handleKillCursors(MongoKillCursors mongoKillCursors) {
        delegate.handleKillCursors(mongoKillCursors);
    }

    @Override
    public void dropDatabase(String database) {
        delegate.dropDatabase(database);
    }

    @Override
    public Collection<Document> getCurrentOperations(MongoQuery query) {
        return delegate.getCurrentOperations(query);
    }

    @Override
    public List<Integer> getVersion() {
        return delegate.getVersion();
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public Clock getClock() {
        return delegate.getClock();
    }

    @Override
    public void setClock(Clock clock) {
        delegate.setClock(clock);
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
        LocalDateTime now = LocalDateTime.now(getClock());
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
