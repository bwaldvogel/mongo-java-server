package de.bwaldvogel.mongo.backend;

import java.util.List;

import de.bwaldvogel.mongo.bson.Document;

public interface Cursor {

    boolean isEmpty();

    long getCursorId();

    List<Document> takeDocuments(int numberToReturn);

}
