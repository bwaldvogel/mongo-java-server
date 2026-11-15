package de.bwaldvogel.mongo.exception;

import java.io.Serial;

import de.bwaldvogel.mongo.bson.Document;

public class IndexNotFoundException extends MongoServerError {

    @Serial
    private static final long serialVersionUID = 1L;

    public IndexNotFoundException(String message) {
        super(ErrorCode.IndexNotFound, message);
    }

    public IndexNotFoundException(Document keys) {
        this("can't find index with key: " + keys.toString(true, "{ ", " }"));
    }

}
