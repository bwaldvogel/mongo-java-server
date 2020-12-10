package de.bwaldvogel.mongo;

import java.time.Clock;
import java.util.Collection;
import java.util.concurrent.CompletionStage;

import de.bwaldvogel.mongo.backend.QueryResult;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.util.FutureUtils;
import de.bwaldvogel.mongo.wire.message.MongoDelete;
import de.bwaldvogel.mongo.wire.message.MongoGetMore;
import de.bwaldvogel.mongo.wire.message.MongoInsert;
import de.bwaldvogel.mongo.wire.message.MongoKillCursors;
import de.bwaldvogel.mongo.wire.message.MongoMessage;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import de.bwaldvogel.mongo.wire.message.MongoUpdate;
import io.netty.channel.Channel;

public interface MongoBackend extends AsyncMongoBackend {

    void handleClose(Channel channel);

    @Override
    default CompletionStage<Void> handleCloseAsync(Channel channel) {
        return FutureUtils.wrap(() -> {
            handleClose(channel);
            return null;
        });
    }

    Document handleCommand(Channel channel, String database, String command, Document query);

    @Override
    default CompletionStage<Document> handleCommandAsync(Channel channel, String database, String command, Document query) {
        return FutureUtils.wrap(() -> handleCommand(channel, database, command, query));
    }

    QueryResult handleQuery(MongoQuery query);

    @Override
    default CompletionStage<QueryResult> handleQueryAsync(MongoQuery query) {
        return FutureUtils.wrap(() -> handleQuery(query));
    }

    QueryResult handleGetMore(long cursorId, int numberToReturn);

    QueryResult handleGetMore(MongoGetMore getMore);

    @Override
    default CompletionStage<QueryResult> handleGetMoreAsync(MongoGetMore getMore) {
        return FutureUtils.wrap(() -> handleGetMore(getMore));
    }

    void handleInsert(MongoInsert insert);

    @Override
    default CompletionStage<Void> handleInsertAsync(MongoInsert insert) {
        return FutureUtils.wrap(() -> {
            handleInsert(insert);
            return null;
        });
    }

    void handleDelete(MongoDelete delete);

    @Override
    default CompletionStage<Void> handleDeleteAsync(MongoDelete delete) {
        return FutureUtils.wrap(() -> {
            handleDelete(delete);
            return null;
        });
    }

    void handleUpdate(MongoUpdate update);

    @Override
    default CompletionStage<Void> handleUpdateAsync(MongoUpdate update) {
        return FutureUtils.wrap(() -> {
            handleUpdate(update);
            return null;
        });
    }

    void handleKillCursors(MongoKillCursors mongoKillCursors);

    @Override
    default CompletionStage<Void> handleKillCursorsAsync(MongoKillCursors mongoKillCursors) {
        return FutureUtils.wrap(() -> {
            handleKillCursors(mongoKillCursors);
            return null;
        });
    }

    Document handleMessage(MongoMessage message);

    @Override
    default CompletionStage<Document> handleMessageAsync(MongoMessage message) {
        return FutureUtils.wrap(() -> handleMessage(message));
    }

    void dropDatabase(String database);

    @Override
    default CompletionStage<Void> dropDatabaseAsync(String database) {
        return FutureUtils.wrap(() -> {
            dropDatabase(database);
            return null;
        });
    }

    Collection<Document> getCurrentOperations(MongoQuery query);

    Document getServerStatus();

    void close();

    @Override
    default CompletionStage<Void> closeAsync() {
        return FutureUtils.wrap(() -> {
            close();
            return null;
        });
    }

    Clock getClock();

    void enableOplog();

    void disableOplog();

    MongoDatabase resolveDatabase(String database);

    MongoBackend version(ServerVersion version);

    void setServerAddress(String serverAddress);

}
