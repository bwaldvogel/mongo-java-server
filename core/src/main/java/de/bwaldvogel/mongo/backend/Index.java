package de.bwaldvogel.mongo.backend;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.KeyConstraintError;

public abstract class Index<P> {

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

    protected Object getKey(Document document) {
        Object value = Utils.getSubdocumentValue(document, key);
        return Utils.normalizeValue(value);
    }

    public abstract void checkAdd(Document document);

    public abstract void add(Document document, P position);

    public abstract P remove(Document document);

    public abstract boolean canHandle(Document query);

    public abstract Iterable<P> getPositions(Document query);

    public abstract long getCount();

    public abstract long getDataSize();

    public abstract void checkUpdate(Document oldDocument, Document newDocument);

    public abstract void updateInPlace(Document oldDocument, Document newDocument) throws KeyConstraintError;

}
