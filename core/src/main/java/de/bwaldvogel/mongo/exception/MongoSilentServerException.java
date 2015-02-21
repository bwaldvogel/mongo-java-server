package de.bwaldvogel.mongo.exception;

/**
 * similar to {@link MongoServerException} but will not be logged as error with stacktrace
 */
public class MongoSilentServerException extends MongoServerException {

    private static final long serialVersionUID = 9148405503786686266L;

    public MongoSilentServerException(String message) {
        super(message);
    }

}
