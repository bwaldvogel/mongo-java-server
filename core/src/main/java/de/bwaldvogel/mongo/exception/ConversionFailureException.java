package de.bwaldvogel.mongo.exception;

public class ConversionFailureException extends MongoServerError {

    private static final long serialVersionUID = 1L;

    public ConversionFailureException(String message) {
        super(ErrorCode.ConversionFailure, message);
    }

}
