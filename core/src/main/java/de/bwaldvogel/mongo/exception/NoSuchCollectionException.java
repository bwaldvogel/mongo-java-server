package de.bwaldvogel.mongo.exception;

public class NoSuchCollectionException extends MongoServerError {

    private static final long serialVersionUID = 1L;

    public NoSuchCollectionException(String collectionName) {
        super(26, "No such collection: " + collectionName);
    }

}
