package de.bwaldvogel.mongo.exception;

import java.io.Serial;

public class KeyConstraintError extends MongoServerError {

    @Serial
    private static final long serialVersionUID = 1L;

    KeyConstraintError(ErrorCode errorCode, String message) {
        super(errorCode, message);
    }
}
