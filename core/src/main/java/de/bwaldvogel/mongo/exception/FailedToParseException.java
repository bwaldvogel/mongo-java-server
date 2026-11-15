package de.bwaldvogel.mongo.exception;

import java.io.Serial;

public class FailedToParseException extends MongoServerError {

    @Serial
    private static final long serialVersionUID = 1L;

    public FailedToParseException(String message) {
        super(ErrorCode.FailedToParse, message);
    }

}
