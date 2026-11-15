package de.bwaldvogel.mongo.exception;

import java.io.Serial;

public class ConflictingUpdateOperatorsException extends MongoServerError {

    @Serial
    private static final long serialVersionUID = 1L;

    public ConflictingUpdateOperatorsException(String updatePath, String conflictingPath) {
        super(ErrorCode.ConflictingUpdateOperators,
            "Updating the path '" + updatePath + "' would create a conflict at '" + conflictingPath + "'");
    }

    @Override
    public boolean shouldPrefixCommandContext() {
        return false;
    }
}
