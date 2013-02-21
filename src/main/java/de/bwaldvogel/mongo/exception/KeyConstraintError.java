package de.bwaldvogel.mongo.exception;

public class KeyConstraintError extends MongoServerError {

    private static final long serialVersionUID = -5036998973183017889L;

    public KeyConstraintError(int errorCode, String message) {
        super(errorCode, message);
    }
}
