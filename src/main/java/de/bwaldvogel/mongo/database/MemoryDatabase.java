package de.bwaldvogel.mongo.database;

import java.util.HashMap;
import java.util.Map;

import de.bwaldvogel.mongo.IDGenerator;
import de.bwaldvogel.mongo.collection.MemoryCollection;
import de.bwaldvogel.mongo.collection.MongoCollection;

public class MemoryDatabase implements MongoDatabase {

    private final Map<String, MongoCollection> collections = new HashMap<String, MongoCollection>();
    private final String databaseName;
    private final IDGenerator idGenerator;

    public MemoryDatabase(IDGenerator idGenerator , String databaseName) {
        this.idGenerator = idGenerator;
        this.databaseName = databaseName;
    }

    @Override
    public MongoCollection resolveCollection( String collectionName ){
        MongoCollection collection = collections.get( collectionName );
        if ( collection == null ) {
            collection = new MemoryCollection( idGenerator , this , collectionName );
            collections.put( collectionName, collection );
        }
        return collection;
    }

    @Override
    public String getName(){
        return databaseName;
    }

}
