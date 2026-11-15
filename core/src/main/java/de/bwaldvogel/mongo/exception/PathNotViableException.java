package de.bwaldvogel.mongo.exception;

import java.io.Serial;

import de.bwaldvogel.mongo.bson.Document;

public class PathNotViableException extends MongoServerError {

    @Serial
    private static final long serialVersionUID = 1L;

    private final String field;

    public PathNotViableException(String field, Document element) {
        super(ErrorCode.PathNotViable, "Cannot create field '" + field + "' in element " + element.toString(true));
        this.field = field;
    }

    public String getField() {
        return field;
    }
}
