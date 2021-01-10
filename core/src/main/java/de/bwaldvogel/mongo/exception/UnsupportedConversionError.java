package de.bwaldvogel.mongo.exception;

import static de.bwaldvogel.mongo.backend.Utils.describeType;

public class UnsupportedConversionError extends MongoServerError {

    private static final long serialVersionUID = 1L;

    public UnsupportedConversionError(Object value, Class<?> targetType) {
        super(ErrorCode.ConversionFailure, "Unsupported conversion from " + describeType(value)
            + " to " + describeType(targetType) + " in $convert with no onError value");
    }
}
