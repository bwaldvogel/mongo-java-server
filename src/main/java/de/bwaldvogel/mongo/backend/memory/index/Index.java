package de.bwaldvogel.mongo.backend.memory.index;

import org.bson.BSONObject;

import de.bwaldvogel.mongo.exception.KeyConstraintError;
import de.bwaldvogel.mongo.exception.MongoServerError;

public abstract class Index {

    private String name;

    public Index(String name) {
        this.name = name;
    }

    public final String getName() {
        return name;
    }

    public abstract void checkAdd(BSONObject document) throws KeyConstraintError;

    public abstract void add(BSONObject document, Integer position) throws KeyConstraintError;

    public abstract Integer remove(BSONObject document);

    public abstract boolean canHandle(BSONObject query);

    public abstract Iterable<Integer> getPositions(BSONObject query);

    public abstract long getCount();

    public abstract long getDataSize();

    protected abstract Object getKeyValue(BSONObject document);

    public abstract void checkUpdate(BSONObject oldDocument, BSONObject newDocument) throws MongoServerError;

    public abstract void updateInPlace(BSONObject oldDocument, BSONObject newDocument) throws KeyConstraintError;

}
