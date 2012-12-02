package de.bwaldvogel.mongo.backend.memory.index;

import org.bson.BSONObject;

import de.bwaldvogel.mongo.exception.KeyConstraintError;

public abstract class Index {

    private String name;

    public Index(String name) {
        this.name = name;
    }

    public final String getName() {
        return name;
    }

    public abstract void checkAdd( BSONObject document ) throws KeyConstraintError;

    public abstract void add( BSONObject document ) throws KeyConstraintError;

    public abstract void remove( BSONObject document );

    public abstract boolean canHandle( BSONObject query );

    public abstract Iterable<Object> getKeys( BSONObject query );

}
