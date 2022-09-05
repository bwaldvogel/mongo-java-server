package de.bwaldvogel.mongo;

import java.time.Clock;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;

import de.bwaldvogel.mongo.backend.QueryResult;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.util.FutureUtils;
import de.bwaldvogel.mongo.wire.message.MongoMessage;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
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

    void closeCursors(List<Long> cursorIds);

    Clock getClock();

    void enableOplog();

    void disableOplog();

    MongoDatabase resolveDatabase(String database);

    MongoBackend version(ServerVersion version);

}
