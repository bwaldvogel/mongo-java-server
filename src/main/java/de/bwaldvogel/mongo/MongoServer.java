package de.bwaldvogel.mongo;

import java.util.HashMap;
import java.util.Map;

import de.bwaldvogel.mongo.collection.MongoCollection;
import de.bwaldvogel.mongo.database.AdminDatabase;
import de.bwaldvogel.mongo.database.MemoryDatabase;
import de.bwaldvogel.mongo.database.MongoDatabase;

public class MongoServer {

    private final IDGenerator idGenerator = new RequestIDGenerator();
    private final AdminDatabase adminDatabase = new AdminDatabase( idGenerator );
    private final Map<String, MongoDatabase> databases = new HashMap<String, MongoDatabase>();

    public MongoCollection getCollection( String fullCollectionName ){
        final int dotIndex = fullCollectionName.indexOf( '.' );
        final String databaseName = fullCollectionName.substring( 0, dotIndex );
        final String collectionName = fullCollectionName.substring( dotIndex + 1 );

        final MongoDatabase database = getDatabase( databaseName );

        return database.resolveCollection( collectionName );
    }

    private MongoDatabase getDatabase( String databaseName ){
        if ( databaseName.equals( "admin" ) )
            return adminDatabase;

        MongoDatabase mongoDatabase = databases.get( databaseName );
        if ( mongoDatabase == null ) {
            mongoDatabase = new MemoryDatabase( idGenerator , databaseName );
            databases.put( databaseName, mongoDatabase );
        }
        return mongoDatabase;

    }

}
