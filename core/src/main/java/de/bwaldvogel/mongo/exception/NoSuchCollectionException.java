package de.bwaldvogel.mongo.exception;

public class NoSuchCollectionException extends MongoServerError {

    private static final long serialVersionUID = 1L;

    private String collectionName;

    public NoSuchCollectionException(String collectionName) {
        super(26, "No such collection: " + collectionName);
        this.collectionName = collectionName;
    }

    public String getCollectionName() {
        return collectionName;
    }

}
