package de.bwaldvogel.mongo.backend;

import java.util.HashMap;
import java.util.Map;

import de.bwaldvogel.mongo.exception.MongoServerError;

enum QueryOperator {

    IN("$in"), //
    NOT_IN("$nin"), //
    NOT("$not"), //
    NOT_EQUALS("$ne"), //
    EQUAL("$eq"), //
    EXISTS("$exists"), //
    GREATER_THAN_OR_EQUAL("$gte"), //
    GREATER_THAN("$gt"), //
    LESS_THAN_OR_EQUAL("$lte"), //
    LESS_THAN("$lt"), //
    MOD("$mod"), //
    SIZE("$size"), //
    ALL("$all"), //
    ELEM_MATCH("$elemMatch"), //
    ;

    private String value;

    QueryOperator(String value) {
        this.value = value;
    }

    String getValue() {
        return value;
    }

    private static final Map<String, QueryOperator> MAP = new HashMap<>();

    static {
        for (QueryOperator operator : QueryOperator.values()) {
            QueryOperator old = MAP.put(operator.getValue(), operator);
            if (old != null)
                throw new IllegalStateException("Duplicate operator value: " + operator.getValue());
        }
    }

    static QueryOperator fromValue(String value) throws MongoServerError {
        QueryOperator op = MAP.get(value);
        if (op == null) {
            throw new MongoServerError(2, "unknown operator: " + value);
        }
        return op;
    }

}
