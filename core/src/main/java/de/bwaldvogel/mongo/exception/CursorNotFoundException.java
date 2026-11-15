package de.bwaldvogel.mongo.exception;

import java.io.Serial;

public class CursorNotFoundException extends MongoServerError {

    @Serial
    private static final long serialVersionUID = 1L;

    public CursorNotFoundException(long cursorId) {
        super(ErrorCode.CursorNotFound, "Cursor id " + cursorId + " does not exist");
    }
}
