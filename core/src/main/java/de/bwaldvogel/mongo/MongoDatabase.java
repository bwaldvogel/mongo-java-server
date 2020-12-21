package de.bwaldvogel.mongo;

import java.util.concurrent.CompletionStage;

import de.bwaldvogel.mongo.backend.CollectionOptions;
import de.bwaldvogel.mongo.backend.QueryResult;
import de.bwaldvogel.mongo.backend.MongoSession;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.oplog.Oplog;
import de.bwaldvogel.mongo.util.FutureUtils;
import de.bwaldvogel.mongo.wire.message.MongoDelete;
import de.bwaldvogel.mongo.wire.message.MongoInsert;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import de.bwaldvogel.mongo.wire.message.MongoUpdate;
import io.netty.channel.Channel;

public interface MongoDatabase extends AsyncMongoDatabase {

    String getDatabaseName();

    void handleClose(Channel channel);

    Document handleCommand(Channel channel, String command, Document query, Oplog oplog, MongoSession mongoSession);

    @Override
    default CompletionStage<Document> handleCommandAsync(Channel channel, String command, Document query, Oplog oplog, MongoSession mongoSession) {
        return FutureUtils.wrap(() -> handleCommand(channel, command, query, oplog, mongoSession));
    }

    QueryResult handleQuery(MongoQuery query);

    @Override
    default CompletionStage<QueryResult> handleQueryAsync(MongoQuery query) {
        return FutureUtils.wrap(() -> handleQuery(query));
    }

    void handleInsert(MongoInsert insert, Oplog oplog);

    @Override
    default CompletionStage<Void> handleInsertAsync(MongoInsert insert, Oplog oplog) {
        return FutureUtils.wrap(() -> {
            handleInsert(insert, oplog);
            return null;
        });
    }

    void handleDelete(MongoDelete delete, Oplog oplog);

    @Override
    default CompletionStage<Void> handleDeleteAsync(MongoDelete delete, Oplog oplog) {
        return FutureUtils.wrap(() -> {
            handleDelete(delete, oplog);
            return null;
        });
    }

    void handleUpdate(MongoUpdate update, Oplog oplog);

    void handleUpdate(MongoUpdate update, Oplog oplog, MongoSession mongoSession);

    @Override
    default CompletionStage<Void> handleUpdateAsync(MongoUpdate update, Oplog oplog) {
        return FutureUtils.wrap(() -> {
            handleUpdate(update, oplog);
            return null;
        });
    }

    boolean isEmpty();

    MongoCollection<?> createCollectionOrThrowIfExists(String collectionName, CollectionOptions options);

    MongoCollection<?> resolveCollection(String collectionName, boolean throwIfNotFound);

    void drop(Oplog oplog);

    void dropCollection(String collectionName, Oplog oplog);

    void moveCollection(MongoDatabase oldDatabase, MongoCollection<?> collection, String newCollectionName);

    void unregisterCollection(String collectionName);

}
