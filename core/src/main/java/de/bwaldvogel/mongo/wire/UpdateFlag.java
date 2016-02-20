package de.bwaldvogel.mongo.wire;

public enum UpdateFlag implements Flag {
    UPSERT(0), MULTI_UPDATE(1);

    private int value;

    UpdateFlag(int bit) {
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

    @Override
    public int addTo(int flags) {
        return flags | value;
    }
}
