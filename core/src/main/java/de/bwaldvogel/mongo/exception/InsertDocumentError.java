package de.bwaldvogel.mongo.exception;

public class InsertDocumentError extends MongoServerError {

    private static final long serialVersionUID = 1L;

    private final int index;

    public InsertDocumentError(MongoServerError mongoServerError, int index) {
        super(mongoServerError.getCode(), mongoServerError.getCodeName(), mongoServerError.getMessageWithoutErrorCode());
        this.index = index;
    }

    public int getIndex() {
        return index;
    }

}
