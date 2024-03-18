package de.bwaldvogel.mongo;

import java.time.Clock;
import java.util.Collection;
import java.util.List;

import de.bwaldvogel.mongo.backend.QueryResult;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.wire.message.MongoMessage;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import io.netty.channel.Channel;

public interface MongoBackend {

    void handleClose(Channel channel);

    Document handleCommand(Channel channel, String database, String command, Document query);

    QueryResult handleQuery(MongoQuery query);

    Document handleMessage(MongoMessage message);

    void dropDatabase(String database);

    Collection<Document> getCurrentOperations(MongoQuery query);

    Document getServerStatus();

    void close();

    void closeCursors(List<Long> cursorIds);

    Clock getClock();

    void enableOplog();

    void disableOplog();

    MongoDatabase resolveDatabase(String database);

    MongoBackend version(MongoVersion version);

}
