package de.bwaldvogel.mongo.backend;

import java.util.List;

import de.bwaldvogel.mongo.bson.Document;

public interface Cursor {

    boolean isEmpty();

    long getId();

    List<Document> takeDocuments(int numberToReturn);

}
