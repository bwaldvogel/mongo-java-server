package de.bwaldvogel.mongo;

import java.time.Clock;
import java.util.Collection;

import de.bwaldvogel.mongo.backend.QueryResult;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.wire.message.*;
import io.netty.channel.Channel;

public interface MongoBackend {

    void handleClose(Channel channel);

    Document handleCommand(Channel channel, String database, String command, Document query);

    QueryResult handleQuery(MongoQuery query);

    QueryResult handleGetMore(long cursorId, int numberToReturn);

    QueryResult handleGetMore(MongoGetMore getMore);

    void handleInsert(MongoInsert insert);

    void handleDelete(MongoDelete delete);

    void handleUpdate(MongoUpdate update);

    void handleKillCursors(MongoKillCursors mongoKillCursors);

    Document handleMessage(MongoMessage message);

    void dropDatabase(String database);

    Collection<Document> getCurrentOperations(MongoQuery query);

    Document getServerStatus();

    void close();

    Clock getClock();

    void enableOplog();

    void disableOplog();

    MongoDatabase resolveDatabase(String database);

    MongoBackend version(ServerVersion version);

}
