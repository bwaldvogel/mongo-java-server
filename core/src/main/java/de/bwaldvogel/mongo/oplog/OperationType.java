package de.bwaldvogel.mongo.oplog;

import java.util.HashMap;
import java.util.Map;

import de.bwaldvogel.mongo.backend.Assert;
import de.bwaldvogel.mongo.exception.BadValueException;
import de.bwaldvogel.mongo.exception.MongoServerError;

public enum OperationType {
    DELETE("d"),
    UPDATE("u"),
    INSERT("i");

    OperationType(String value) { this.value = value; }
    public String getValue() { return value; }

    private String value;

    private static final Map<String, OperationType> MAP = new HashMap<>();

    static {
        for (OperationType operationType : OperationType.values()) {
            OperationType old = MAP.put(operationType.getValue(), operationType);
            Assert.isNull(old, () -> "Duplicate operation type value: " + operationType.getValue());
        }
    }

    static OperationType fromValue(String value) throws MongoServerError {
        OperationType operationType = MAP.get(value);
        if (operationType == null) {
            throw new BadValueException("unknown operation type: " + value);
        }
        return operationType;
    }
}
