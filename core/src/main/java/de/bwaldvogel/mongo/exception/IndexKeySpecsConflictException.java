package de.bwaldvogel.mongo.exception;

import de.bwaldvogel.mongo.backend.Index;
import de.bwaldvogel.mongo.bson.Json;

public class IndexKeySpecsConflictException extends MongoServerError {

    private static final long serialVersionUID = 1L;

    public IndexKeySpecsConflictException(Index<?> requestedIndex, Index<?> existingIndex) {
        super(ErrorCode.IndexKeySpecsConflict,
            "An existing index has the same name as the requested index. " +
                "When index names are not specified, they are auto generated and can cause conflicts. " +
                "Please refer to our documentation. " +
                "Requested index: " + toJson(requestedIndex) + ", " +
                "existing index: " + toJson(existingIndex));
    }

    private static String toJson(Index<?> index) {
        return Json.toCompactJsonValue(index.toIndexDescription());
    }

}
