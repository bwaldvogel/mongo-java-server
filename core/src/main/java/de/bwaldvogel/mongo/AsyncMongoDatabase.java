package de.bwaldvogel.mongo;

import java.util.concurrent.CompletionStage;

import de.bwaldvogel.mongo.backend.QueryResult;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.oplog.Oplog;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import io.netty.channel.Channel;

public interface AsyncMongoDatabase {

    CompletionStage<Document> handleCommandAsync(Channel channel, String command, Document query, Oplog oplog);

    CompletionStage<QueryResult> handleQueryAsync(MongoQuery query);

}
