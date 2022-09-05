package de.bwaldvogel.mongo;

import java.util.concurrent.CompletionStage;

import de.bwaldvogel.mongo.backend.QueryResult;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.wire.message.MongoMessage;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import io.netty.channel.Channel;

public interface AsyncMongoBackend {

    CompletionStage<Void> handleCloseAsync(Channel channel);

    CompletionStage<Document> handleCommandAsync(Channel channel, String database, String command, Document query);

    CompletionStage<QueryResult> handleQueryAsync(MongoQuery query);

    CompletionStage<Document> handleMessageAsync(MongoMessage message);

    CompletionStage<Void> dropDatabaseAsync(String database);

}
