package de.bwaldvogel.mongo.backend;

import java.util.List;

import de.bwaldvogel.mongo.bson.Document;

public interface Cursor {
    long getId();

    boolean isEmpty();

    List<Document> takeDocuments(int numberToReturn);
}
