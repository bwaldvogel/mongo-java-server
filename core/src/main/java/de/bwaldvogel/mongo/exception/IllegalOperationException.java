package de.bwaldvogel.mongo.exception;

public class IllegalOperationException extends MongoServerError {

    private static final long serialVersionUID = 1L;

    public IllegalOperationException(String message) {
        super(ErrorCode.IllegalOperation, message);
    }

}
