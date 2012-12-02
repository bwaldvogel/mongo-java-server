package de.bwaldvogel.mongo.backend.memory;

import org.bson.BSONObject;

import de.bwaldvogel.mongo.exception.MongoServerException;
import de.bwaldvogel.mongo.wire.message.MongoDelete;
import de.bwaldvogel.mongo.wire.message.MongoInsert;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import de.bwaldvogel.mongo.wire.message.MongoUpdate;

public interface MongoDatabase {

    String getDatabaseName();

    void handleClose( int clientId );

    BSONObject handleCommand( int clientId , String command, BSONObject query ) throws MongoServerException;

    Iterable<BSONObject> handleQuery( MongoQuery query ) throws MongoServerException;

    void handleInsert( MongoInsert insert );

    void handleDelete( MongoDelete delete );

    void handleUpdate( MongoUpdate update );

    boolean isEmpty();

}
