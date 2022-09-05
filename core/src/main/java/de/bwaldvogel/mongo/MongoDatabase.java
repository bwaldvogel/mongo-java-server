package de.bwaldvogel.mongo;

import java.util.concurrent.CompletionStage;

import de.bwaldvogel.mongo.backend.CollectionOptions;
import de.bwaldvogel.mongo.backend.QueryResult;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.oplog.Oplog;
import de.bwaldvogel.mongo.util.FutureUtils;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import io.netty.channel.Channel;

public interface MongoDatabase extends AsyncMongoDatabase {

    String getDatabaseName();

    void handleClose(Channel channel);

    Document handleCommand(Channel channel, String command, Document query, Oplog oplog);

    @Override
    default CompletionStage<Document> handleCommandAsync(Channel channel, String command, Document query, Oplog oplog) {
        return FutureUtils.wrap(() -> handleCommand(channel, command, query, oplog));
    }

    QueryResult handleQuery(MongoQuery query);

    @Override
    default CompletionStage<QueryResult> handleQueryAsync(MongoQuery query) {
        return FutureUtils.wrap(() -> handleQuery(query));
    }

    boolean isEmpty();

    MongoCollection<?> createCollectionOrThrowIfExists(String collectionName, CollectionOptions options);

    MongoCollection<?> resolveCollection(String collectionName, boolean throwIfNotFound);

    void drop(Oplog oplog);

    void dropCollection(String collectionName, Oplog oplog);

    void moveCollection(MongoDatabase oldDatabase, MongoCollection<?> collection, String newCollectionName);

    void unregisterCollection(String collectionName);

}
