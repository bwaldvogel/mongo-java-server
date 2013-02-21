package de.bwaldvogel.mongo.wire;

public enum QueryFlag implements Flag {
    SLAVE_OK(2);

    private int value;

    private QueryFlag(int bit) {
        this.value = 1 << bit;
    }

    @Override
    public boolean isSet(int flags) {
        return (flags & value) == value;
    }

    @Override
    public int removeFrom(int flags) {
        return flags - value;
    }
}
