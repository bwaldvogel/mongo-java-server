package de.bwaldvogel.mongo.backend;

import java.util.Collection;

import org.bson.BSONObject;

import de.bwaldvogel.mongo.exception.MongoServerException;
import de.bwaldvogel.mongo.wire.message.MongoDelete;
import de.bwaldvogel.mongo.wire.message.MongoInsert;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import de.bwaldvogel.mongo.wire.message.MongoUpdate;

public interface MongoBackend {

    void handleConnect( int clientId );

    void handleClose( int clientId );

    BSONObject handleCommand( int clientId , String database , String command , BSONObject query ) throws MongoServerException;

    Iterable<BSONObject> handleQuery( MongoQuery query ) throws MongoServerException;

    void handleInsert( MongoInsert insert ) throws MongoServerException;

    void handleDelete( MongoDelete delete ) throws MongoServerException;

    void handleUpdate( MongoUpdate update ) throws MongoServerException;

    Collection<BSONObject> getCurrentOperations( MongoQuery query );

}
