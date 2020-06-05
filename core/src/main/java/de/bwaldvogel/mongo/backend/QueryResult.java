package de.bwaldvogel.mongo.backend;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import de.bwaldvogel.mongo.bson.Document;

public class QueryResult implements Iterable<Document> {

    private final Iterable<Document> documents;
    private final long cursorId;

    public QueryResult() {
        this(Collections.emptyList(), EmptyCursor.get());
    }

    public QueryResult(Iterable<Document> documents, Cursor cursor) {
        this(documents, cursor.getId());
    }

    public QueryResult(Iterable<Document> documents, long cursorId) {
        this.documents = documents;
        this.cursorId = cursorId;
    }

    public QueryResult(Iterable<Document> documents) {
        this(documents, EmptyCursor.get());
    }

    public List<Document> collectDocuments() {
        if (documents instanceof List<?>) {
            return Collections.unmodifiableList((List<Document>) documents);
        } else {
            List<Document> documents = new ArrayList<>();
            for (Document document : this) {
                documents.add(document);
            }
            return documents;
        }
    }

    @Override
    public Iterator<Document> iterator() {
        return documents.iterator();
    }

    public long getCursorId() {
        return cursorId;
    }
}
