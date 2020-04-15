package de.bwaldvogel.mongo.backend;

import de.bwaldvogel.mongo.bson.Document;

public interface Cursor {

    boolean isEmpty();

    long getCursorId();

    Document pollDocument();

}
