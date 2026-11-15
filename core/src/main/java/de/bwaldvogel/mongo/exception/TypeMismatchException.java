package de.bwaldvogel.mongo.exception;

import java.io.Serial;

public class TypeMismatchException extends MongoServerError {

    @Serial
    private static final long serialVersionUID = 1L;

    public TypeMismatchException(String message) {
        super(ErrorCode.TypeMismatch, message);
    }

}
