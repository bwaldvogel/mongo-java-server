package de.bwaldvogel.mongo.oplog;

import java.util.HashMap;
import java.util.Map;

import de.bwaldvogel.mongo.backend.Assert;

public enum OperationType {
    DELETE("d"),
    UPDATE("u"),
    INSERT("i"),
    COMMAND("c"),
    INVALIDATE("invalidate");

    private final String code;

    OperationType(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    private static final Map<String, OperationType> MAP = new HashMap<>();

    static {
        for (OperationType operationType : OperationType.values()) {
            OperationType old = MAP.put(operationType.getCode(), operationType);
            Assert.isNull(old, () -> "Duplicate operation type value: " + operationType.getCode());
        }
    }

    static OperationType fromCode(String code) {
        OperationType operationType = MAP.get(code);
        if (operationType == null) {
            throw new IllegalArgumentException("unknown operation type: " + code);
        }
        return operationType;
    }

    String getDescription() {
        switch (this) {
            case DELETE:
                return "delete";
            case INSERT:
                return "insert";
            case UPDATE:
                return "update";
            case COMMAND:
                return "command";
            case INVALIDATE:
                return "invalidate";
        }
        return null;
    }
}
