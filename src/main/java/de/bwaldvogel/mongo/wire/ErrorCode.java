package de.bwaldvogel.mongo.wire;

import java.util.HashMap;
import java.util.Map;

public enum ErrorCode {
    BAD_VALUE(2),
    UNKNOWN_ERROR(8),
    INVALID_BSON(22),
    COMMAND_NOT_FOUND(59),
    WRITE_CONCERN_FAILED(64),
    MULTIPLE_ERRORS_OCCURRED(65);

    private final int id;

    private static Map<Integer, ErrorCode> byIdMap = new HashMap<Integer, ErrorCode>();

    static {
        for (final ErrorCode opCode : values()) {
            byIdMap.put(Integer.valueOf(opCode.id), opCode);
        }
    }

    private ErrorCode(final int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public static ErrorCode getById(int id) {
        return byIdMap.get(Integer.valueOf(id));
    }

}
