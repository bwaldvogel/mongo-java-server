package de.bwaldvogel.mongo.backend;

import java.util.HashSet;
import java.util.Set;

import org.bson.BSONObject;

import de.bwaldvogel.mongo.exception.MongoServerException;
import de.bwaldvogel.mongo.exception.NoSuchCommandException;
import de.bwaldvogel.mongo.wire.message.MongoDelete;
import de.bwaldvogel.mongo.wire.message.MongoInsert;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import de.bwaldvogel.mongo.wire.message.MongoUpdate;

public class ReadOnlyProxy implements MongoBackend {

    private static final Set<String> allowedCommands = new HashSet<String>();
    static {
        allowedCommands.add( "isMaster" );
        allowedCommands.add( "listDatabases" );
        allowedCommands.add( "count" );
    }

    private MongoBackend backend;

    public ReadOnlyProxy(MongoBackend backend) {
        this.backend = backend;
    }

    public static class ReadOnlyException extends MongoServerException {

        private static final long serialVersionUID = 4781141056923033645L;

        public ReadOnlyException(String message) {
            super( message );
        }

    }

    @Override
    public void handleClose( int clientId ) {
        backend.handleClose( clientId );
    }

    @Override
    public BSONObject handleCommand( int clientId , String database , String command , BSONObject query ) throws MongoServerException {
        if ( allowedCommands.contains( command ) ) {
            return backend.handleCommand( clientId, database, command, query );
        }
        throw new NoSuchCommandException( command );
    }

    public Iterable<BSONObject> handleQuery( MongoQuery query ) throws MongoServerException {
        return backend.handleQuery( query );
    }

    public void handleInsert( MongoInsert insert ) throws MongoServerException {
        throw new ReadOnlyException( "insert not allowed" );
    }

    public void handleDelete( MongoDelete delete ) throws MongoServerException {
        throw new ReadOnlyException( "delete not allowed" );
    }

    public void handleUpdate( MongoUpdate update ) throws MongoServerException {
        throw new ReadOnlyException( "update not allowed" );
    }

}
