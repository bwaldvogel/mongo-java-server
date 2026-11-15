package de.bwaldvogel.mongo.exception;

import java.io.Serial;
import java.util.Arrays;

public class MongoServerError extends MongoServerException {

    @Serial
    private static final long serialVersionUID = 1L;

    private final String message;
    private final int errorCode;
    private final String codeName;

    public MongoServerError(int errorCode, String message) {
        this(errorCode, "Location" + errorCode, message);
    }

    public MongoServerError(ErrorCode errorCode, String message) {
        this(errorCode, message, null);
    }

    public MongoServerError(ErrorCode errorCode, String message, Throwable cause) {
        this(errorCode.getValue(), errorCode.getName(), message, cause);
    }

    public MongoServerError(int errorCode, String codeName, String message) {
        this(errorCode, codeName, message, null);
    }

    public MongoServerError(int errorCode, String codeName, String message, Throwable cause) {
        super("[Error " + errorCode + "] " + message, cause);
        this.errorCode = errorCode;
        this.codeName = codeName;
        this.message = message;
    }

    public int getCode() {
        return errorCode;
    }

    public String getCodeName() {
        return codeName;
    }

    @Override
    public String getMessageWithoutErrorCode() {
        return message;
    }

    public boolean shouldPrefixCommandContext() {
        return true;
    }

    public boolean hasCode(ErrorCode... errorCodes) {
        return Arrays.stream(errorCodes).anyMatch(code -> getCode() == code.getValue());
    }
}
