package de.bwaldvogel.mongo;

import java.util.concurrent.CompletionStage;

import de.bwaldvogel.mongo.backend.MongoSession;
import de.bwaldvogel.mongo.backend.QueryResult;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.oplog.Oplog;
import de.bwaldvogel.mongo.wire.message.MongoDelete;
import de.bwaldvogel.mongo.wire.message.MongoInsert;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import de.bwaldvogel.mongo.wire.message.MongoUpdate;
import io.netty.channel.Channel;

public interface AsyncMongoDatabase {

    CompletionStage<Document> handleCommandAsync(Channel channel, String command, Document query, Oplog oplog, MongoSession mongoSession);

    CompletionStage<QueryResult> handleQueryAsync(MongoQuery query);

    CompletionStage<Void> handleInsertAsync(MongoInsert insert, Oplog oplog);

    CompletionStage<Void> handleDeleteAsync(MongoDelete delete, Oplog oplog);

    CompletionStage<Void> handleUpdateAsync(MongoUpdate update, Oplog oplog);
}
