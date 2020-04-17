package de.bwaldvogel.mongo;

import java.time.Clock;
import java.util.Collection;
import java.util.List;

import de.bwaldvogel.mongo.backend.QueryResult;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.wire.message.MongoDelete;
import de.bwaldvogel.mongo.wire.message.MongoGetMore;
import de.bwaldvogel.mongo.wire.message.MongoInsert;
import de.bwaldvogel.mongo.wire.message.MongoKillCursors;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import de.bwaldvogel.mongo.wire.message.MongoUpdate;
import io.netty.channel.Channel;

public interface MongoBackend {

    void handleClose(Channel channel);

    Document handleCommand(Channel channel, String database, String command, Document query);

    QueryResult handleQuery(MongoQuery query);

    QueryResult handleGetMore(MongoGetMore getMore);

    void handleInsert(MongoInsert insert);

    void handleDelete(MongoDelete delete);

    void handleUpdate(MongoUpdate update);

    void handleKillCursors(MongoKillCursors mongoKillCursors);

    void dropDatabase(String database);

    Collection<Document> getCurrentOperations(MongoQuery query);

    List<Integer> getVersion();

    void close();

    Clock getClock();

    void setClock(Clock clock);

}
