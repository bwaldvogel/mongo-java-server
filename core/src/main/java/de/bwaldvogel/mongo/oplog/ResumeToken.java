package de.bwaldvogel.mongo.oplog;

import de.bwaldvogel.mongo.bson.BsonTimestamp;
import de.bwaldvogel.mongo.bson.Document;

public final class ResumeToken {

    private final long value;
    private final boolean inclusive;

    private ResumeToken(long value) {
        this(value, false);
    }

    private ResumeToken(long value, boolean inclusive) {
        this.value = value;
        this.inclusive = inclusive;
    }

    public ResumeToken(BsonTimestamp timestamp) {
        this(timestamp.getValue());
    }

    public ResumeToken inclusive() {
        return new ResumeToken(value, true);
    }

    private static ResumeToken fromHexString(String hexString) {
        return new ResumeToken(Long.parseLong(hexString, 16));
    }

    public static ResumeToken fromDocument(Document document) {
        return fromHexString((String) document.get("_data"));
    }

    public String toHexString() {
        return Long.toHexString(value);
    }

    public boolean isAfter(ResumeToken other) {
        if (other.inclusive) {
            return value >= other.value;
        } else {
            return value > other.value;
        }
    }

}
