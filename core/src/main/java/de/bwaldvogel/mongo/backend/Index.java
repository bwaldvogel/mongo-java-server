package de.bwaldvogel.mongo.backend;

import org.bson.BSONObject;

import de.bwaldvogel.mongo.exception.KeyConstraintError;
import de.bwaldvogel.mongo.exception.MongoServerError;

public abstract class Index<KEY> {

    protected final String key;
    protected final boolean ascending;

    protected Index(String key, boolean ascending) {
        this.key = key;
        this.ascending = ascending;
    }

    public String getName() {
        if (key.equals(Constants.ID_FIELD)) {
            return Constants.ID_INDEX_NAME;
        } else {
            return key + "_" + (ascending ? "1" : "-1");
        }
    }

    protected Object getKeyValue(BSONObject document) {
        Object value = Utils.getSubdocumentValue(document, key);
        return Utils.normalizeValue(value);
    }

    public abstract void checkAdd(BSONObject document) throws KeyConstraintError;

    public abstract void add(BSONObject document, KEY key) throws KeyConstraintError;

    public abstract KEY remove(BSONObject document);

    public abstract boolean canHandle(BSONObject query);

    public abstract Iterable<KEY> getKeys(BSONObject query);

    public abstract long getCount();

    public abstract long getDataSize();

    public abstract void checkUpdate(BSONObject oldDocument, BSONObject newDocument) throws MongoServerError;

    public abstract void updateInPlace(BSONObject oldDocument, BSONObject newDocument) throws KeyConstraintError;

}
