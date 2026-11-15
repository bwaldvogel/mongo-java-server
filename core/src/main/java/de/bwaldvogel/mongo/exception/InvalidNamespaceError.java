package de.bwaldvogel.mongo.exception;

import java.io.Serial;

public class InvalidNamespaceError extends MongoServerError {

    @Serial
    private static final long serialVersionUID = 1L;

    public InvalidNamespaceError(String namespace) {
        super(ErrorCode.InvalidNamespace, "Invalid system namespace: " + namespace);
    }

}
