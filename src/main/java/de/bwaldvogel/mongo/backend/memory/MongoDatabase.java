package de.bwaldvogel.mongo.backend.memory;

import org.bson.BSONObject;
import org.jboss.netty.channel.Channel;

import de.bwaldvogel.mongo.exception.MongoServerException;
import de.bwaldvogel.mongo.wire.message.MongoDelete;
import de.bwaldvogel.mongo.wire.message.MongoInsert;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import de.bwaldvogel.mongo.wire.message.MongoUpdate;

public interface MongoDatabase {

    String getDatabaseName();

    void handleClose( Channel channel );

    BSONObject handleCommand( Channel channel , String command , BSONObject query ) throws MongoServerException;

    Iterable<BSONObject> handleQuery( MongoQuery query ) throws MongoServerException;

    void handleInsert( MongoInsert insert );

    void handleDelete( MongoDelete delete );

    void handleUpdate( MongoUpdate update );

    boolean isEmpty();

}
