package de.bwaldvogel.mongo;

import java.util.Collection;
import java.util.List;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerException;
import de.bwaldvogel.mongo.wire.message.MongoDelete;
import de.bwaldvogel.mongo.wire.message.MongoInsert;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import de.bwaldvogel.mongo.wire.message.MongoUpdate;
import io.netty.channel.Channel;

public interface MongoBackend {

    void handleClose(Channel channel);

    Document handleCommand(Channel channel, String database, String command, Document query) throws MongoServerException;

    Iterable<Document> handleQuery(MongoQuery query) throws MongoServerException;

    void handleInsert(MongoInsert insert) throws MongoServerException;

    void handleDelete(MongoDelete delete) throws MongoServerException;

    void handleUpdate(MongoUpdate update) throws MongoServerException;

    void dropDatabase(String database) throws MongoServerException;

    Collection<Document> getCurrentOperations(MongoQuery query);

    List<Integer> getVersion();

    void close();

}
