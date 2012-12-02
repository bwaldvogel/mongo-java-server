package de.bwaldvogel.mongo.backend;

import org.bson.BSONObject;

import de.bwaldvogel.mongo.exception.MongoServerException;
import de.bwaldvogel.mongo.exception.NoSuchCommandException;
import de.bwaldvogel.mongo.wire.message.MongoDelete;
import de.bwaldvogel.mongo.wire.message.MongoInsert;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import de.bwaldvogel.mongo.wire.message.MongoUpdate;

public interface MongoBackend {

    void handleClose( int clientId );

    Iterable<BSONObject> handleQuery( MongoQuery query ) throws MongoServerException, NoSuchCommandException;

    void handleInsert( MongoInsert insert );

    void handleDelete( MongoDelete delete );

    void handleUpdate( MongoUpdate update );

}
