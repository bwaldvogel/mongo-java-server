package de.bwaldvogel.mongo.backend;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import de.bwaldvogel.mongo.bson.Document;

public class Cursor implements Iterable<Document> {

    public static final long EMPTY_CURSOR_ID = 0L;

    private final long cursorId;
    private final Queue<Document> documents = new LinkedList<>();

    public Cursor(Iterable<Document> documents, long cursorId) {
        this.cursorId = cursorId;
        for (Document document : documents) {
            this.documents.add(document);
        }
    }

    public Cursor() {
        this(Collections.emptyList(), EMPTY_CURSOR_ID);
    }

    public int documentsCount() {
        return documents.size();
    }

    public boolean isEmpty() {
        return documents.isEmpty();
    }

    public long getCursorId() {
        return cursorId;
    }

    public Queue<Document> getDocuments() {
        return documents;
    }

    @Override
    public Iterator<Document> iterator() {
        return documents.iterator();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(id: " + cursorId + ")";
    }
}
