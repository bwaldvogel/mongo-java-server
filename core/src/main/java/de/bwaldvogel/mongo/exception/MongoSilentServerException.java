package de.bwaldvogel.mongo.exception;

import java.io.Serial;

/**
 * similar to {@link MongoServerException} but will not be logged as error with stacktrace
 */
public class MongoSilentServerException extends MongoServerException {

    @Serial
    private static final long serialVersionUID = 1L;

    public MongoSilentServerException(String message) {
        super(message);
        setLogError(false);
    }

}
