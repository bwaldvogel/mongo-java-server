package de.bwaldvogel.mongo.backend;

import de.bwaldvogel.mongo.bson.Document;

import java.util.List;

public class InMemoryCursor extends AbstractCursor {

    public InMemoryCursor(long cursorId, List<Document> remainingDocuments) {
        super(cursorId, remainingDocuments);
        Assert.notEmpty(remainingDocuments);
    }

    public int documentsCount() {
        return remainingDocuments.size();
    }

    @Override
    public boolean isEmpty() {
        return remainingDocuments.isEmpty();
    }

    @Override
    public List<Document> takeDocuments(int numberToReturn) {
        Assert.isTrue(numberToReturn > 0, () -> "Illegal number to return: " + numberToReturn);
        int toIndex = Math.min(remainingDocuments.size(), numberToReturn);
        List<Document> documents = remainingDocuments.subList(0, toIndex);
        remainingDocuments = remainingDocuments.subList(documents.size(), remainingDocuments.size());
        return documents;
    }

}
