package de.bwaldvogel.mongo.exception;


public class MongoServerException extends Exception {

    private static final long serialVersionUID = 3357301041846925271L;

    public MongoServerException(String message) {
        super(message);
    }

    public MongoServerException(String message, Throwable cause) {
        super(message, cause);
    }

}
