package de.bwaldvogel.mongo;

import java.util.concurrent.CompletionStage;

import de.bwaldvogel.mongo.backend.QueryResult;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.wire.message.MongoDelete;
import de.bwaldvogel.mongo.wire.message.MongoGetMore;
import de.bwaldvogel.mongo.wire.message.MongoInsert;
import de.bwaldvogel.mongo.wire.message.MongoKillCursors;
import de.bwaldvogel.mongo.wire.message.MongoMessage;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import de.bwaldvogel.mongo.wire.message.MongoUpdate;
import io.netty.channel.Channel;

public interface AsyncMongoBackend {

    CompletionStage<Void> handleCloseAsync(Channel channel);

    CompletionStage<Document> handleCommandAsync(Channel channel, String database, String command, Document query);

    CompletionStage<QueryResult> handleQueryAsync(MongoQuery query);

    CompletionStage<QueryResult> handleGetMoreAsync(MongoGetMore getMore);

    CompletionStage<Void> handleInsertAsync(MongoInsert insert);

    CompletionStage<Void> handleDeleteAsync(MongoDelete delete);

    CompletionStage<Void> handleUpdateAsync(MongoUpdate update);

    CompletionStage<Void> handleKillCursorsAsync(MongoKillCursors mongoKillCursors);

    CompletionStage<Document> handleMessageAsync(MongoMessage message);

    CompletionStage<Void> dropDatabaseAsync(String database);

    CompletionStage<Void> closeAsync();
}
