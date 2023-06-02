package de.bwaldvogel.mongo.exception;

public class CannotTraverseElementException extends MongoServerError {

    private static final long serialVersionUID = 1L;

    public CannotTraverseElementException() {
        super(ErrorCode.PathNotViable, "cannot traverse element");
    }

}
