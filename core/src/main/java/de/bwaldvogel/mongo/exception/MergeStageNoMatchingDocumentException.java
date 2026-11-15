package de.bwaldvogel.mongo.exception;

import java.io.Serial;

public class MergeStageNoMatchingDocumentException extends MongoServerError {

    @Serial
    private static final long serialVersionUID = 1L;

    public MergeStageNoMatchingDocumentException() {
        super(ErrorCode.MergeStageNoMatchingDocument,
            "$merge could not find a matching document in the target collection for at least one document in the source collection");
    }
}
