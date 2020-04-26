package de.bwaldvogel.mongo;

import de.bwaldvogel.mongo.backend.CollectionOptions;
import de.bwaldvogel.mongo.backend.QueryResult;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.oplog.Oplog;
import de.bwaldvogel.mongo.wire.message.MongoDelete;
import de.bwaldvogel.mongo.wire.message.MongoGetMore;
import de.bwaldvogel.mongo.wire.message.MongoInsert;
import de.bwaldvogel.mongo.wire.message.MongoKillCursors;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import de.bwaldvogel.mongo.wire.message.MongoUpdate;
import io.netty.channel.Channel;

public interface MongoDatabase {

    String getDatabaseName();

    void handleClose(Channel channel);

    Document handleCommand(Channel channel, String command, Document query, Oplog oplog);

    QueryResult handleQuery(MongoQuery query);

    QueryResult handleGetMore(MongoGetMore getMore);

    void handleKillCursors(MongoKillCursors killCursors);

    void handleInsert(MongoInsert insert, Oplog oplog);

    void handleDelete(MongoDelete delete);

    void handleUpdate(MongoUpdate update);

    boolean isEmpty();

    MongoCollection<?> createCollectionOrThrowIfExists(String collectionName, CollectionOptions options);

    MongoCollection<?> resolveCollection(String collectionName, boolean throwIfNotFound);

    void drop();

    void dropCollection(String collectionName);

    void moveCollection(MongoDatabase oldDatabase, MongoCollection<?> collection, String newCollectionName);

    void unregisterCollection(String collectionName);

}
