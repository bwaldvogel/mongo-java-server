package de.bwaldvogel.mongo.exception;

public class BadValueException extends MongoServerError {

    private static final long serialVersionUID = 1L;

    private final boolean shouldPrefixCommandContext;

    public BadValueException(String message) {
        this(message, true);
    }

    public BadValueException(String message, boolean shouldPrefixCommandContext) {
        super(ErrorCode.BadValue, message);
        this.shouldPrefixCommandContext = shouldPrefixCommandContext;
    }

    @Override
    public boolean shouldPrefixCommandContext() {
        return shouldPrefixCommandContext;
    }
}
