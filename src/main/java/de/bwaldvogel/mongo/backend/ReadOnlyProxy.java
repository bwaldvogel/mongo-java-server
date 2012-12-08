package de.bwaldvogel.mongo.backend;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.bson.BSONObject;
import org.jboss.netty.channel.Channel;

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
        allowedCommands.add( "dbstats" );
        allowedCommands.add( "collstats" );
        allowedCommands.add( "serverStatus" );
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

    public void handleClose( Channel channel ) {
        backend.handleClose( channel );
    }

    @Override
    public BSONObject handleCommand( Channel channel , String database , String command , BSONObject query ) throws MongoServerException {
        if ( allowedCommands.contains( command ) ) {
            return backend.handleCommand( channel, database, command, query );
        }
        throw new NoSuchCommandException( command );
    }

    @Override
    public Collection<BSONObject> getCurrentOperations( MongoQuery query ) {
        return backend.getCurrentOperations( query );
    }

    @Override
    public Iterable<BSONObject> handleQuery( MongoQuery query ) throws MongoServerException {
        return backend.handleQuery( query );
    }

    @Override
    public void handleInsert( MongoInsert insert ) throws MongoServerException {
        throw new ReadOnlyException( "insert not allowed" );
    }

    @Override
    public void handleDelete( MongoDelete delete ) throws MongoServerException {
        throw new ReadOnlyException( "delete not allowed" );
    }

    @Override
    public void handleUpdate( MongoUpdate update ) throws MongoServerException {
        throw new ReadOnlyException( "update not allowed" );
    }

}
