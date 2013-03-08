package de.bwaldvogel.mongo.exception;

public class MongoServerError extends MongoServerException {

    private static final long serialVersionUID = 4998311355923688257L;
    private int errorCode;

    public MongoServerError(int errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public int getCode() {
        return errorCode;
    }

}
