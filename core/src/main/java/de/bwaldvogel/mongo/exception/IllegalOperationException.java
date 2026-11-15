package de.bwaldvogel.mongo.exception;

import java.io.Serial;

public class IllegalOperationException extends MongoServerError {

    @Serial
    private static final long serialVersionUID = 1L;

    public IllegalOperationException(String message) {
        super(ErrorCode.IllegalOperation, message);
    }

}
