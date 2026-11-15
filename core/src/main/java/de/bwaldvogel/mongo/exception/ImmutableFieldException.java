package de.bwaldvogel.mongo.exception;

import java.io.Serial;

public class ImmutableFieldException extends MongoServerError {

    @Serial
    private static final long serialVersionUID = 1L;

    public ImmutableFieldException(String message) {
        super(ErrorCode.ImmutableField, message);
    }

}
