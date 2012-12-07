package de.bwaldvogel.mongo.backend.memory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.TreeMap;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.jboss.netty.channel.Channel;

import de.bwaldvogel.mongo.backend.MongoBackend;
import de.bwaldvogel.mongo.exception.MongoServerException;
import de.bwaldvogel.mongo.exception.NoSuchCommandException;
import de.bwaldvogel.mongo.wire.MongoWireProtocolHandler;
import de.bwaldvogel.mongo.wire.message.Message;
import de.bwaldvogel.mongo.wire.message.MongoDelete;
import de.bwaldvogel.mongo.wire.message.MongoInsert;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import de.bwaldvogel.mongo.wire.message.MongoUpdate;

public class MemoryBackend implements MongoBackend {

    private final TreeMap<String, MongoDatabase> databases = new TreeMap<String, MongoDatabase>();

    protected BSONObject handleAdminCommand( BSONObject query ) throws NoSuchCommandException {

        String command = query.keySet().iterator().next();

        if ( command.equals( "ismaster" ) || command.equals( "isMaster" ) ) {
            BSONObject reply = new BasicBSONObject( "ismaster" , Boolean.TRUE );
            reply.put( "maxBsonObjectSize", Integer.valueOf( MongoWireProtocolHandler.MAX_BSON_OBJECT_SIZE ) );
            reply.put( "localTime", new Date() );
            reply.put( "ok", Integer.valueOf( "1" ) );
            return reply;
        }
        else if ( command.equals( "listDatabases" ) ) {
            BSONObject response = new BasicBSONObject();
            List<BSONObject> dbs = new ArrayList<BSONObject>();
            for ( MongoDatabase db : databases.values() ) {
                BasicBSONObject dbObj = new BasicBSONObject( "name" , db.getDatabaseName() );
                dbObj.put( "empty", Boolean.valueOf( db.isEmpty() ) );
                dbs.add( dbObj );
            }
            response.put( "databases", dbs );
            response.put( "ok", Integer.valueOf( 1 ) );
            return response;
        }
        else {
            throw new NoSuchCommandException( command );
        }
    }

    private synchronized MongoDatabase resolveDatabase( Message message ) {
        return resolveDatabase( message.getDatabaseName() );
    }

    private synchronized MongoDatabase resolveDatabase( String database ) {
        MongoDatabase db = databases.get( database );
        if ( db == null ) {
            db = new MemoryDatabase( this , database );
            databases.put( database, db );
        }
        return db;
    }

    @Override
    public void handleClose( Channel channel ) {
        for ( MongoDatabase db : databases.values() ) {
            db.handleClose( channel );
        }
    }

    @Override
    public BSONObject handleCommand( Channel channel , String databaseName , String command , BSONObject query ) throws MongoServerException {
        if ( databaseName.equals( "admin" ) ) {
            return handleAdminCommand( query );
        }
        else {
            MongoDatabase db = resolveDatabase( databaseName );
            return db.handleCommand( channel, command, query );
        }
    }

    @Override
    public Collection<BSONObject> getCurrentOperations( MongoQuery query ) {
        // TODO
        return Collections.emptyList();
    }

    @Override
    public Iterable<BSONObject> handleQuery( MongoQuery query ) throws MongoServerException {
        MongoDatabase db = resolveDatabase( query );
        return db.handleQuery( query );
    }

    @Override
    public void handleInsert( MongoInsert insert ) throws MongoServerException {
        MongoDatabase db = resolveDatabase( insert );
        db.handleInsert( insert );
    }

    @Override
    public void handleDelete( MongoDelete delete ) throws MongoServerException {
        MongoDatabase db = resolveDatabase( delete );
        db.handleDelete( delete );
    }

    @Override
    public void handleUpdate( MongoUpdate update ) throws MongoServerException {
        MongoDatabase db = resolveDatabase( update );
        db.handleUpdate( update );
    }

    public void dropDatabase( MemoryDatabase memoryDatabase ) {
        databases.remove( memoryDatabase.getDatabaseName() );
    }

}
