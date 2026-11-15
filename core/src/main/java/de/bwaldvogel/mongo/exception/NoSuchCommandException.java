package de.bwaldvogel.mongo.exception;

import java.io.Serial;

public class NoSuchCommandException extends MongoServerError {

    @Serial
    private static final long serialVersionUID = 1L;

    public NoSuchCommandException(String command) {
        super(ErrorCode.CommandNotFound, "no such command: '" + command + "'");
    }

}
