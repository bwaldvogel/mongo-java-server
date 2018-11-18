package de.bwaldvogel.mongo.exception;

import java.util.List;
import java.util.stream.Collectors;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.backend.Index;
import de.bwaldvogel.mongo.bson.Json;

public class DuplicateKeyError extends KeyConstraintError {

    private static final long serialVersionUID = 1L;

    public DuplicateKeyError(Index<?> index, MongoCollection<?> collection, List<?> values) {
        super(11000, "E11000 duplicate key error collection: " + collection.getFullName()
            + " index: " + index.getName() + " dup key: " + valuesToString(values));
    }

    private static String valuesToString(List<?> values) {
        return values.stream()
            .map(value -> ": " + Json.toJsonValue(value))
            .collect(Collectors.joining(", ", "{ ", " }"));
    }

}
