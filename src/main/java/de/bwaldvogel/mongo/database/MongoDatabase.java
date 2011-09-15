package de.bwaldvogel.mongo.database;

import de.bwaldvogel.mongo.collection.MongoCollection;

public interface MongoDatabase {
    public MongoCollection resolveCollection( String collectionName );

    public String getName();
}
