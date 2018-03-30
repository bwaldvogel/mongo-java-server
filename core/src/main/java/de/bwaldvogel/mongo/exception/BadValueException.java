package de.bwaldvogel.mongo.exception;

public class BadValueException extends MongoServerError {

    private static final long serialVersionUID = 1L;

    private static final int ERROR_CODE = 2;
    private static final String CODE_NAME = "BadValue";

    public BadValueException(String message) {
        super(ERROR_CODE, CODE_NAME, message);
    }

}
