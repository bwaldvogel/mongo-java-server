package de.bwaldvogel.mongo.exception;

import java.io.Serial;

public class InvalidIdFieldError extends MongoServerError {

    @Serial
    private static final long serialVersionUID = 1L;

    public InvalidIdFieldError(String message) {
        super(ErrorCode.InvalidIdField, message);
    }

}
