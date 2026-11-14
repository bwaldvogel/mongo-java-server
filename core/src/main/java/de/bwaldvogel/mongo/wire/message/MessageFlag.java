package de.bwaldvogel.mongo.wire.message;

public enum MessageFlag {
    CHECKSUM_PRESENT(0),
    MORE_TO_COME(1),
    EXHAUST_ALLOWED(16),
    ;

    private final int value;

    MessageFlag(int bit) {
        this.value = 1 << bit;
    }

    int getValue() {
        return value;
    }

    public boolean isSet(int flags) {
        return (flags & value) == value;
    }

    public int removeFrom(int flags) {
        return flags - value;
    }

}
