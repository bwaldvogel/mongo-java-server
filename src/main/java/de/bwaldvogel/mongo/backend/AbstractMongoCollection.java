package de.bwaldvogel.mongo.backend;

import de.bwaldvogel.mongo.MongoCollection;

public abstract class AbstractMongoCollection implements MongoCollection {

    private String collectionName;
    private String databaseName;

    protected AbstractMongoCollection(String databaseName, String collectionName) {
        this.databaseName = databaseName;
        this.collectionName = collectionName;
    }

    public String getFullName() {
        return databaseName + "." + getCollectionName();
    }

    public String getCollectionName() {
        return collectionName;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" + getFullName() + ")";
    }

}
