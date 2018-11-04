package de.bwaldvogel.mongo.exception;

public class MongoServerException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public MongoServerException(String message) {
        super(validateMessage(message));
    }

    public MongoServerException(String message, Throwable cause) {
        super(validateMessage(message), cause);
    }

    private static String validateMessage(String message) {
        if (message == null || message.isEmpty()) {
            throw new IllegalArgumentException("illegal error message");
        }
        return message;
    }

}
