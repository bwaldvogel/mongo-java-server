package de.bwaldvogel.mongo.bson;

import java.time.Instant;
import java.util.Objects;

public final class BsonTimestamp implements Bson, Comparable<BsonTimestamp> {

    private static final long serialVersionUID = 1L;

    private final long value;

    protected BsonTimestamp() {
        this(0);
    }

    public BsonTimestamp(long value) {
        this.value = value;
    }

    public BsonTimestamp(Instant instant, int increment) {
        value = (instant.getEpochSecond() << 32) | (increment & 0xFFFFFFFFL);
    }

    public long getValue() {
        return value;
    }

    public int getTime() {
        return (int) (value >> 32);
    }

    public int getInc() {
        return (int) value;
    }

    @Override
    public String toString() {
        return "BsonTimestamp[value=" + getValue()
            + ", seconds=" + getTime()
            + ", inc=" + getInc()
            + "]";
    }

    @Override
    public int compareTo(BsonTimestamp other) {
        return Long.compareUnsigned(value, other.value);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BsonTimestamp other = (BsonTimestamp) o;
        return this.value == other.value;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(value);
    }

}
