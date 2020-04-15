package de.bwaldvogel.mongo.backend;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;

import de.bwaldvogel.mongo.bson.Document;

public class InMemoryCursor implements Cursor {

    private final long cursorId;
    private final Queue<Document> remainingDocuments;

    public InMemoryCursor(long cursorId, Collection<Document> remainingDocuments) {
        this.cursorId = cursorId;
        Assert.notEmpty(remainingDocuments);
        this.remainingDocuments = new LinkedList<>(remainingDocuments);
    }

    public int documentsCount() {
        return remainingDocuments.size();
    }

    @Override
    public boolean isEmpty() {
        return remainingDocuments.isEmpty();
    }

    @Override
    public long getCursorId() {
        return cursorId;
    }

    @Override
    public Document pollDocument() {
        return remainingDocuments.poll();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(id: " + cursorId + ")";
    }
}
