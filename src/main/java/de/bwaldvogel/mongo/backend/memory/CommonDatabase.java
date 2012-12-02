package de.bwaldvogel.mongo.backend.memory;

import java.util.Collections;

import org.bson.BSONObject;

import de.bwaldvogel.mongo.exception.MongoServerException;
import de.bwaldvogel.mongo.exception.NoSuchCommandException;
import de.bwaldvogel.mongo.wire.message.MongoQuery;

abstract class CommonDatabase implements MongoDatabase {

    private final String databaseName;

    public CommonDatabase(String databaseName) {
        this.databaseName = databaseName;
    }

    @Override
    public final String getDatabaseName(){
        return databaseName;
    }

    @Override
    public final Iterable<BSONObject> handleQuery( MongoQuery query ) throws MongoServerException, NoSuchCommandException{
        String collectionName = query.getCollectionName();
        if ( collectionName.equals( "$cmd" ) ) {
            return Collections.singletonList( handleCommand( query.getClientId(), query.getQuery() ) );
        }
        return doHandleQuery( query );
    }

    private BSONObject handleCommand( int clientId , BSONObject query ) throws MongoServerException, NoSuchCommandException{
        String command = query.keySet().iterator().next();
        return handleCommand( clientId, command, query );
    }

    protected abstract Iterable<BSONObject> doHandleQuery( MongoQuery query ) throws MongoServerException;

    protected abstract BSONObject handleCommand( int clientId , String command , BSONObject query )
            throws NoSuchCommandException, MongoServerException;
}
