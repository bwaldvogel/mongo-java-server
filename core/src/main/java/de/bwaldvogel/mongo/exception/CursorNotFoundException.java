package de.bwaldvogel.mongo.exception;

public class CursorNotFoundException extends MongoServerError {
    public CursorNotFoundException(String message) {
        super(ErrorCode.CursorNotFound, message);
    }
}
