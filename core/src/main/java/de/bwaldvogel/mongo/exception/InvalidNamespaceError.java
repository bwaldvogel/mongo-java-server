package de.bwaldvogel.mongo.exception;

public class InvalidNamespaceError extends MongoServerError {

    private static final long serialVersionUID = 1L;

    public InvalidNamespaceError(String namespace) {
        super(ErrorCode.InvalidNamespace, "Invalid system namespace: " + namespace);
    }

}
