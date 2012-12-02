package de.bwaldvogel.mongo.backend.memory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import de.bwaldvogel.mongo.exception.MongoServerException;
import de.bwaldvogel.mongo.exception.NoSuchCommandException;
import de.bwaldvogel.mongo.exception.ReservedCollectionNameException;
import de.bwaldvogel.mongo.wire.message.ClientRequest;
import de.bwaldvogel.mongo.wire.message.MongoDelete;
import de.bwaldvogel.mongo.wire.message.MongoInsert;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import de.bwaldvogel.mongo.wire.message.MongoUpdate;

public class MemoryDatabase extends CommonDatabase {

    private static final Logger log = Logger.getLogger( MemoryDatabase.class );

    private static final String ID_FIELD = "_id";

    private Map<String, MemoryCollection> collections = new HashMap<String, MemoryCollection>();
    private Map<Integer, MongoServerException> lastExceptions = new HashMap<Integer, MongoServerException>();
    private MemoryCollection namespaces;

    private MemoryBackend backend;

    public MemoryDatabase(MemoryBackend backend , String databaseName) {
        super( databaseName );
        this.backend = backend;
        namespaces = new MemoryCollection( getDatabaseName() , "system.namespaces" , "name" );
        collections.put( "system.namespaces", namespaces );
    }

    @Override
    protected Iterable<BSONObject> doHandleQuery( MongoQuery query ) throws MongoServerException{
        MemoryCollection collection = resolveCollection( query );
        return collection.handleQuery( query.getQuery() );
    }

    private synchronized MemoryCollection resolveCollection( ClientRequest request ) throws MongoServerException{
        String collectionName = request.getCollectionName();
        checkCollectionName( collectionName );
        MemoryCollection collection = collections.get( collectionName );
        if ( collection == null ) {
            collection = new MemoryCollection( getDatabaseName() , collectionName , ID_FIELD );
            collections.put( collectionName, collection );
            namespaces.addDocument( new BasicBSONObject( "name" , collection.getFullName() ) );
        }
        return collection;
    }

    private void checkCollectionName( String collectionName ) throws MongoServerException{
        if ( collectionName.contains( "$" ) ) {
            throw new ReservedCollectionNameException( collectionName );
        }
    }

    public MongoServerException getLastException( int clientId ){
        return lastExceptions.get( Integer.valueOf( clientId ) );
    }

    @Override
    public boolean isEmpty(){
        return collections.isEmpty();
    }

    @Override
    public void handleClose( int clientId ){
        lastExceptions.remove( Integer.valueOf( clientId ) );
    }

    protected BSONObject handleCommand( int clientId , String command , BSONObject query ) throws MongoServerException, NoSuchCommandException{
        if ( command.equals( "count" ) ) {
            String collection = query.get( command ).toString();
            BSONObject response = new BasicBSONObject();
            MemoryCollection coll = collections.get( collection );
            if ( coll == null ) {
                response.put( "missing", Boolean.TRUE );
                response.put( "n", Integer.valueOf( 0 ) );
            }
            else {
                response.put( "n", Integer.valueOf( coll.getCount() ) );
            }
            response.put( "ok", Integer.valueOf( 1 ) );
            return response;
        }
        else if ( command.equals( "getlasterror" ) ) {
            Iterator<String> it = query.keySet().iterator();
            it.next();
            String subCommand = it.next();
            if ( subCommand.equals( "w" ) ) {
                if ( lastExceptions != null ) {
                    MongoServerException ex = lastExceptions.remove( Integer.valueOf( clientId ) );
                    if ( ex != null )
                        throw ex;
                }
                BSONObject response = new BasicBSONObject();
                return response;
            }
            else {
                log.error( "unknown query: " + query );
            }
        }
        else if ( command.equals( "drop" ) ) {

            String collectionName = query.get( "drop" ).toString();
            MemoryCollection collection = collections.remove( collectionName );

            BSONObject response = new BasicBSONObject();
            if ( collection == null ) {
                response.put( "errmsg", "ns not found" );
                response.put( "ok", Integer.valueOf( 0 ) );
            }
            else {
                namespaces.removeDocument( new BasicBSONObject( "name" , collection.getFullName() ) );
                response.put( "nIndexesWas", Integer.valueOf( collection.getNumIndexes() ) );
                response.put( "ns", collection.getFullName() );
                response.put( "ok", Integer.valueOf( 1 ) );
            }

            return response;
        }
        else if ( command.equals( "dropDatabase" ) ) {
            backend.dropDatabase( this );
            BSONObject response = new BasicBSONObject( "dropped" , getDatabaseName() );
            response.put( "ok", Integer.valueOf( 1 ) );
            return response;
        }
        else {
            log.error( "unknown query: " + query );
        }
        throw new NoSuchCommandException( command );
    }

    @Override
    public void handleInsert( MongoInsert insert ){
        try {
            MemoryCollection collection = resolveCollection( insert );
            collection.handleInsert( insert );
        }
        catch ( MongoServerException e ) {
            log.error( "failed to insert " + insert, e );
            lastExceptions.put( Integer.valueOf( insert.getClientId() ), e );
        }
    }

    @Override
    public void handleDelete( MongoDelete delete ){
        try {
            MemoryCollection collection = resolveCollection( delete );
            collection.handleDelete( delete );
        }
        catch ( MongoServerException e ) {
            log.error( "failed to delete " + delete, e );
            lastExceptions.put( Integer.valueOf( delete.getClientId() ), e );
        }
    }

    @Override
    public void handleUpdate( MongoUpdate update ){
        try {
            MemoryCollection collection = resolveCollection( update );
            collection.handleUpdate( update );
        }
        catch ( MongoServerException e ) {
            log.error( "failed to update " + update, e );
            lastExceptions.put( Integer.valueOf( update.getClientId() ), e );
        }
    }
}
