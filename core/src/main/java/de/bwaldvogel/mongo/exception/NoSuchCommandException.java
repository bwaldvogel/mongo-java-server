package de.bwaldvogel.mongo.exception;

public class NoSuchCommandException extends MongoServerError {

    private static final long serialVersionUID = 1L;

    public NoSuchCommandException(String command) {
        super(59, "no such cmd: " + command);
    }

}
