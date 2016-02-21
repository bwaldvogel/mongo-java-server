package de.bwaldvogel.mongo.backend;

import org.bson.Document;

import de.bwaldvogel.mongo.exception.KeyConstraintError;
import de.bwaldvogel.mongo.exception.MongoServerException;

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

    protected Object getKeyValue(Document document) {
        Object value = Utils.getSubdocumentValue(document, key);
        return Utils.normalizeValue(value);
    }

    public abstract void checkAdd(Document document) throws MongoServerException;

    public abstract void add(Document document, KEY key) throws MongoServerException;

    public abstract KEY remove(Document document);

    public abstract boolean canHandle(Document query);

    public abstract Iterable<KEY> getKeys(Document query);

    public abstract long getCount();

    public abstract long getDataSize();

    public abstract void checkUpdate(Document oldDocument, Document newDocument) throws MongoServerException;

    public abstract void updateInPlace(Document oldDocument, Document newDocument) throws KeyConstraintError;

}
