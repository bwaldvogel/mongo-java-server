package de.bwaldvogel.mongo.backend;

import io.netty.channel.Channel;

import java.util.Collection;

import org.bson.BSONObject;

import de.bwaldvogel.mongo.exception.MongoServerException;
import de.bwaldvogel.mongo.wire.message.MongoDelete;
import de.bwaldvogel.mongo.wire.message.MongoInsert;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import de.bwaldvogel.mongo.wire.message.MongoUpdate;

public interface MongoBackend {

    void handleClose(Channel channel);

    BSONObject handleCommand(Channel channel, String database, String command, BSONObject query)
            throws MongoServerException;

    Iterable<BSONObject> handleQuery(MongoQuery query) throws MongoServerException;

    void handleInsert(MongoInsert insert) throws MongoServerException;

    void handleDelete(MongoDelete delete) throws MongoServerException;

    void handleUpdate(MongoUpdate update) throws MongoServerException;

    Collection<BSONObject> getCurrentOperations(MongoQuery query);

    int[] getVersion();

}
