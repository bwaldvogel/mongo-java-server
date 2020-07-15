package de.bwaldvogel.mongo.backend;

import java.util.List;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.KeyConstraintError;

public class EmptyIndex<P> extends Index<P> {

    public EmptyIndex(String name, List<IndexKey> keys) {
        super(name, keys, true);
    }

    @Override
    public P getPosition(Document document) {
        return null;
    }

    @Override
    public void checkAdd(Document document, MongoCollection<P> collection) {
        // ignore
    }

    @Override
    public void add(Document document, P position, MongoCollection<P> collection) {
        // ignore
    }

    @Override
    public P remove(Document document) {
        return null;
    }

    @Override
    public boolean canHandle(Document query) {
        return false;
    }

    @Override
    public Iterable<P> getPositions(Document query) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getCount() {
        return 0;
    }

    @Override
    public long getDataSize() {
        return 0;
    }

    @Override
    public void checkUpdate(Document oldDocument, Document newDocument, MongoCollection<P> collection) {
        // ignore
    }

    @Override
    public void updateInPlace(Document oldDocument, Document newDocument, P position, MongoCollection<P> collection) throws KeyConstraintError {
        // ignore
    }

    @Override
    public void drop() {
    }
}
