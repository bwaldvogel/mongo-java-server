package de.bwaldvogel.mongo.backend;

import org.bson.BSONObject;

import de.bwaldvogel.mongo.exception.KeyConstraintError;
import de.bwaldvogel.mongo.exception.MongoServerError;

public abstract class Index<P extends Position> {

    public abstract String getName();

    public abstract void checkAdd(BSONObject document) throws KeyConstraintError;

    public abstract void add(BSONObject document, P position) throws KeyConstraintError;

    public abstract P remove(BSONObject document);

    public abstract boolean canHandle(BSONObject query);

    public abstract Iterable<P> getPositions(BSONObject query);

    public abstract long getCount();

    public abstract long getDataSize();

    protected abstract Object getKeyValue(BSONObject document);

    public abstract void checkUpdate(BSONObject oldDocument, BSONObject newDocument) throws MongoServerError;

    public abstract void updateInPlace(BSONObject oldDocument, BSONObject newDocument) throws KeyConstraintError;

}
