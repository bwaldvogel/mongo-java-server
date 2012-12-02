package de.bwaldvogel.mongo.wire.message;

import org.bson.BSONObject;

import de.bwaldvogel.mongo.backend.MongoServerBackend;
import de.bwaldvogel.mongo.exception.MongoServerException;
import de.bwaldvogel.mongo.exception.NoSuchCommandException;

public class MongoServer {

    private final MongoServerBackend backend;

    public MongoServer(MongoServerBackend backend) {
        this.backend = backend;
    }

    public void handleClose( int clientId ){
        backend.handleClose( clientId );
    }

    public Iterable<BSONObject> handleQuery( MongoQuery query ) throws MongoServerException, NoSuchCommandException{
        return backend.handleQuery( query );
    }

    public void handleInsert( MongoInsert insert ){
        backend.handleInsert( insert );
    }

    public void handleDelete( MongoDelete delete ){
        backend.handleDelete( delete );
    }

    public void handleUpdate( MongoUpdate update ){
        backend.handleUpdate( update );
    }

}
