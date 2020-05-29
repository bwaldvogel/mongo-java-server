package de.bwaldvogel.mongo.backend;

import java.util.Collections;
import java.util.List;

import de.bwaldvogel.mongo.bson.Document;

public abstract class AbstractCursor implements Cursor {

    protected final long id;
    protected List<Document> remainingDocuments;

    protected AbstractCursor(long id, List<Document> remainingDocuments) {
        this.id = id;
        this.remainingDocuments = Collections.unmodifiableList(remainingDocuments);
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public boolean isEmpty() {
        return remainingDocuments.isEmpty();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(id: " + id + ")";
    }

}
