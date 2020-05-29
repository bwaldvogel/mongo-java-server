package de.bwaldvogel.mongo.backend;

import java.util.Collections;
import java.util.List;

import de.bwaldvogel.mongo.bson.Document;

public abstract class AbstractCursor implements Cursor {

    protected final long cursorId;
    protected List<Document> remainingDocuments;

    protected AbstractCursor(long cursorId, List<Document> remainingDocuments) {
        this.cursorId = cursorId;
        this.remainingDocuments = Collections.unmodifiableList(remainingDocuments);
    }

    @Override
    public long getCursorId() {
        return cursorId;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(id: " + cursorId + ")";
    }

}
