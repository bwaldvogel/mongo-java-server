package de.bwaldvogel.mongo;

import de.bwaldvogel.mongo.backend.CollectionOptions;
import de.bwaldvogel.mongo.backend.DatabaseResolver;
import de.bwaldvogel.mongo.backend.QueryResult;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.oplog.Oplog;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import io.netty.channel.Channel;

public interface MongoDatabase {

    String getDatabaseName();

    void handleClose(Channel channel);

    Document handleCommand(Channel channel, String command, Document query, DatabaseResolver databaseResolver, Oplog oplog);

    QueryResult handleQuery(MongoQuery query);

    boolean isEmpty();

    default MongoCollection<?> createCollectionOrThrowIfExists(String collectionName) {
        return createCollectionOrThrowIfExists(collectionName, CollectionOptions.withDefaults());
    }

    MongoCollection<?> createCollectionOrThrowIfExists(String collectionName, CollectionOptions options);

    MongoCollection<?> resolveCollection(String collectionName, boolean throwIfNotFound);

    void drop(Oplog oplog);

    void dropCollection(String collectionName, Oplog oplog);

    void moveCollection(MongoDatabase oldDatabase, MongoCollection<?> collection, String newCollectionName);

    void unregisterCollection(String collectionName);

}
