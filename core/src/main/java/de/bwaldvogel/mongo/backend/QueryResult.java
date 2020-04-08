package de.bwaldvogel.mongo.backend;

import java.util.Collections;
import java.util.Iterator;

import de.bwaldvogel.mongo.bson.Document;

public class QueryResult implements Iterable<Document> {

    private final Iterable<Document> documents;
    private final long cursorId;

    public QueryResult() {
        this(Collections.emptyList(), 0);
    }

    public QueryResult(Iterable<Document> documents, long cursorId) {
        this.documents = documents;
        this.cursorId = cursorId;
    }

    public QueryResult(Iterable<Document> documents) {
        this(documents, 0);
    }

    public Iterable<Document> getDocuments() {
        return documents;
    }

    @Override
    public Iterator<Document> iterator() {
        return documents.iterator();
    }

    public long getCursorId() {
        return cursorId;
    }
}
