package de.bwaldvogel.mongo.backend;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
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
    public List<Document> takeDocuments(int numberToReturn) {
        Assert.isTrue(numberToReturn > 0, () -> "Illegal number to return: " + numberToReturn);
        List<Document> documents = new ArrayList<>();
        while (documents.size() < numberToReturn) {
            Document nextDocument = remainingDocuments.poll();
            if (nextDocument == null) {
                return documents;
            }
            documents.add(nextDocument);
        }
        return documents;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(id: " + cursorId + ")";
    }
}
