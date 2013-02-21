package de.bwaldvogel.mongo.exception;

public class ReservedCollectionNameError extends MongoServerError {

    private static final long serialVersionUID = 7385539454918648055L;

    public ReservedCollectionNameError(String message) {
        super(10093, message);
    }

}
