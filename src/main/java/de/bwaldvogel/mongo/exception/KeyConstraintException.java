package de.bwaldvogel.mongo.exception;


public class KeyConstraintException extends MongoServerException {

    private static final long serialVersionUID = -5036998973183017889L;

    public KeyConstraintException(int errorCode , String message) {
        super( errorCode , message );
    }
}
