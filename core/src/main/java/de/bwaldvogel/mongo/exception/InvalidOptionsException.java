package de.bwaldvogel.mongo.exception;

import java.io.Serial;

public class InvalidOptionsException extends MongoServerError {

    @Serial
    private static final long serialVersionUID = 1L;

    public InvalidOptionsException(String message) {
        super(ErrorCode.InvalidOptions, message);
    }

}
