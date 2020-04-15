package de.bwaldvogel.mongo.backend;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;

import de.bwaldvogel.mongo.bson.Document;

public class Cursor {

    public static final long EMPTY_CURSOR_ID = 0L;

    private final long cursorId;
    private final Queue<Document> remainingDocuments;

    public Cursor(long cursorId, Collection<Document> remainingDocuments) {
        this.cursorId = cursorId;
        Assert.notEmpty(remainingDocuments);
        this.remainingDocuments = new LinkedList<>(remainingDocuments);
    }

    public Cursor() {
        this.cursorId = EMPTY_CURSOR_ID;
        this.remainingDocuments = new LinkedList<>();
    }

    public int documentsCount() {
        return remainingDocuments.size();
    }

    public boolean isEmpty() {
        return remainingDocuments.isEmpty();
    }

    public long getCursorId() {
        return cursorId;
    }

    public Document pollDocument() {
        return remainingDocuments.poll();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(id: " + cursorId + ")";
    }
}
