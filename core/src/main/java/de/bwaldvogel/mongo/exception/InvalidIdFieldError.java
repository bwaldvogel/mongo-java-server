package de.bwaldvogel.mongo.exception;

public class InvalidIdFieldError extends MongoServerError {

    private static final long serialVersionUID = 1L;

    public InvalidIdFieldError(String message) {
        super(ErrorCode.InvalidIdField, message);
    }

}
