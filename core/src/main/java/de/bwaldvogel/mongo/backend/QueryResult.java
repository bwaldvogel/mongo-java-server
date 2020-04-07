package de.bwaldvogel.mongo.backend;

import java.util.Collections;
import java.util.Iterator;

import de.bwaldvogel.mongo.bson.Document;

public class QueryResult<T extends Document> implements Iterable<T> {

    private Iterable<T> documents;
    private int remainingDocuments;
    private long cursorId;

    public QueryResult() {
        this(Collections.emptyList(), 0);
    }

    public QueryResult(Iterable<T> documents, long cursorId) {
        this.documents = documents;
        this.cursorId = cursorId;
    }

    public QueryResult(Iterable<T> documents) {
        this(documents, 0);
    }

    public Iterable<T> getDocuments() {
        return documents;
    }

    public int getRemainingDocuments() {
        return remainingDocuments;
    }

    @Override
    public Iterator<T> iterator() {
        return documents.iterator();
    }

    public long getCursorId() {
        return cursorId;
    }
}
