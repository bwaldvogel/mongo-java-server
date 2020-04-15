package de.bwaldvogel.mongo.oplog;

public enum OperationType {
    DELETE("d"),
    UPDATE("u"),
    INSERT("i");

    private final String value;
    OperationType(String value) { this.value = value; }
    public String getValue() { return value; }
}
