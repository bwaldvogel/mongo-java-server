package de.bwaldvogel.mongo.exception;

import java.io.Serial;

public class NamespaceExistsException extends MongoServerError {

    @Serial
    private static final long serialVersionUID = 1L;

    public NamespaceExistsException(String message) {
        super(ErrorCode.NamespaceExists, message);
    }

}
