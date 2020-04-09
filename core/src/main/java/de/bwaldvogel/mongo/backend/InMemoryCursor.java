package de.bwaldvogel.mongo.backend;

import java.util.Collections;
import java.util.List;

import de.bwaldvogel.mongo.bson.Document;

public class InMemoryCursor implements Cursor {

    private final long cursorId;
    private List<Document> remainingDocuments;

    public InMemoryCursor(long cursorId, List<Document> remainingDocuments) {
        this.cursorId = cursorId;
        Assert.notEmpty(remainingDocuments);
        this.remainingDocuments = Collections.unmodifiableList(remainingDocuments);
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
    public List<Document> takeDocuments(int numberToReturn) {
        Assert.isTrue(numberToReturn > 0, () -> "Illegal number to return: " + numberToReturn);
        int toIndex = Math.min(remainingDocuments.size(), numberToReturn);
        List<Document> documents = remainingDocuments.subList(0, toIndex);
        remainingDocuments = remainingDocuments.subList(documents.size(), remainingDocuments.size());
        return documents;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(id: " + cursorId + ")";
    }
}
