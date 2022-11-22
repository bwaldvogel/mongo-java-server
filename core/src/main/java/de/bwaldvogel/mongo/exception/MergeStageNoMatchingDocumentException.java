package de.bwaldvogel.mongo.exception;

public class MergeStageNoMatchingDocumentException extends MongoServerError {

    private static final long serialVersionUID = 1L;

    public MergeStageNoMatchingDocumentException() {
        super(ErrorCode.MergeStageNoMatchingDocument,
            "$merge could not find a matching document in the target collection for at least one document in the source collection");
    }
}
