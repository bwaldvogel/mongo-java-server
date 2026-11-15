package de.bwaldvogel.mongo.exception;

import java.io.Serial;

public class DollarPrefixedFieldNameException extends MongoServerError {

    @Serial
    private static final long serialVersionUID = 1L;

    public DollarPrefixedFieldNameException(String message) {
        super(ErrorCode.DollarPrefixedFieldName, message);
    }

}
