package de.bwaldvogel.mongo.bson;

public class BsonTimestamp implements Bson, Comparable<BsonTimestamp> {

    private static final long serialVersionUID = 1L;
    private final long value;

    /**
     * Construct a new instance with a null time and a 0 increment.
     */
    public BsonTimestamp() {
        value = 0;
    }

    /**
     * Construct a new instance for the given value, which combines the time in seconds and the increment as a single long value.
     *
     * @param value the timetamp as a single long value
     * @since 3.5
     */
    public BsonTimestamp(final long value) {
        this.value = value;
    }

    /**
     * Construct a new instance for the given time and increment.
     *
     * @param seconds the number of seconds since the epoch
     * @param increment  the increment.
     */
    public BsonTimestamp(final long seconds, final int increment) {
        value = (seconds << 32) | (increment & 0xFFFFFFFFL);
    }

    /**
     * Gets the value of the timestamp.
     *
     * @return the timestamp value
     * @since 3.5
     */
    public long getValue() {
        return value;
    }

    /**
     * Gets the time in seconds since epoch.
     *
     * @return an int representing time in seconds since epoch
     */
    public int getTime() {
        return (int) (value >> 32);
    }

    /**
     * Gets the increment value.
     *
     * @return an incrementing ordinal for operations within a given second
     */
    public int getInc() {
        return (int) value;
    }

    @Override
    public String toString() {
        return "Timestamp{"
            + "value=" + getValue()
            + ", seconds=" + getTime()
            + ", inc=" + getInc()
            + '}';
    }

    @Override
    public int compareTo(final BsonTimestamp ts) {
        return Long.compareUnsigned(value, ts.value);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BsonTimestamp timestamp = (BsonTimestamp) o;

        if (value != timestamp.value) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return (int) (value ^ (value >>> 32));
    }
}
