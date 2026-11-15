package de.bwaldvogel.mongo.exception;

import java.io.Serial;

public class ConversionFailureException extends MongoServerError {

    @Serial
    private static final long serialVersionUID = 1L;

    public ConversionFailureException(String message) {
        super(ErrorCode.ConversionFailure, message);
    }

}
