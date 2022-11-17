package de.bwaldvogel.mongo.exception;

import java.util.UUID;

import de.bwaldvogel.mongo.MongoCollection;

public class IndexBuildFailedException extends MongoServerError {
    private static final long serialVersionUID = 1L;

    public IndexBuildFailedException(MongoServerError e, MongoCollection<?> collection) {
        super(e.getCode(), e.getCodeName(), "Index build failed: " + UUID.randomUUID() + ": " +
            "Collection " + collection.getFullName() + " ( " + collection.getUuid() + " ) :: caused by :: "
            + e.getMessageWithoutErrorCode(), e);

    }
}
