package de.bwaldvogel.mongo.backend.memory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.TreeMap;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import de.bwaldvogel.mongo.backend.MongoBackend;
import de.bwaldvogel.mongo.exception.MongoServerException;
import de.bwaldvogel.mongo.exception.NoSuchCommandException;
import de.bwaldvogel.mongo.wire.message.Message;
import de.bwaldvogel.mongo.wire.message.MongoDelete;
import de.bwaldvogel.mongo.wire.message.MongoInsert;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import de.bwaldvogel.mongo.wire.message.MongoUpdate;

public class MemoryBackend implements MongoBackend {

    private static final Integer MAX_BSON_OBJECT_SIZE = Integer.valueOf( 16777216 );

    private final TreeMap<String, MongoDatabase> databases = new TreeMap<String, MongoDatabase>();

    public MemoryBackend() {
    }

    protected BSONObject handleAdminCommand( BSONObject query ) throws NoSuchCommandException{

        String command = query.keySet().iterator().next();

        if ( command.equals( "ismaster" ) || command.equals( "isMaster" ) ) {
            BSONObject reply = new BasicBSONObject( "ismaster" , Boolean.TRUE );
            reply.put( "maxBsonObjectSize", MAX_BSON_OBJECT_SIZE );
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

    private synchronized MongoDatabase resolveDatabase( Message message ){
        String database = message.getDatabaseName();
        MongoDatabase db = databases.get( database );
        if ( db == null ) {
            db = new MemoryDatabase( this , database );
            databases.put( database, db );
        }
        return db;
    }

    @Override
    public void handleClose( int clientId ){
        for ( MongoDatabase db : databases.values() ) {
            db.handleClose( clientId );
        }
    }

    @Override
    public Iterable<BSONObject> handleQuery( MongoQuery query ) throws MongoServerException, NoSuchCommandException{
        if ( query.getFullCollectionName().equals( "admin.$cmd" ) ) {
            return Collections.singletonList( handleAdminCommand( query.getQuery() ) );
        }
        MongoDatabase db = resolveDatabase( query );
        return db.handleQuery( query );
    }

    @Override
    public void handleInsert( MongoInsert insert ){
        MongoDatabase db = resolveDatabase( insert );
        db.handleInsert( insert );
    }

    @Override
    public void handleDelete( MongoDelete delete ){
        MongoDatabase db = resolveDatabase( delete );
        db.handleDelete( delete );
    }

    @Override
    public void handleUpdate( MongoUpdate update ){
        MongoDatabase db = resolveDatabase( update );
        db.handleUpdate( update );
    }

    public void dropDatabase( MemoryDatabase memoryDatabase ){
        databases.remove( memoryDatabase.getDatabaseName() );
    }

}
