package de.bwaldvogel.mongo.oplog;

import de.bwaldvogel.mongo.bson.BsonTimestamp;
import de.bwaldvogel.mongo.bson.Document;

public final class OplogPosition {

    private final BsonTimestamp timestamp;
    private final boolean inclusive;

    private OplogPosition(long value) {
        this(new BsonTimestamp(value), false);
    }

    private OplogPosition(BsonTimestamp timestamp, boolean inclusive) {
        this.timestamp = timestamp;
        this.inclusive = inclusive;
    }

    public OplogPosition(BsonTimestamp timestamp) {
        this(timestamp.getValue());
    }

    public OplogPosition inclusive() {
        return new OplogPosition(timestamp, true);
    }

    private static OplogPosition fromHexString(String hexString) {
        return new OplogPosition(Long.parseLong(hexString, 16));
    }

    public static OplogPosition fromTimestamp(BsonTimestamp timestamp) {
        return new OplogPosition(timestamp);
    }

    public static OplogPosition fromDocument(Document document) {
        return fromHexString((String) document.get(OplogDocumentFields.ID_DATA_KEY));
    }

    public String toHexString() {
        return Long.toHexString(timestamp.getValue());
    }

    public boolean isAfter(OplogPosition other) {
        int comparison = timestamp.compareTo(other.timestamp);
        if (other.inclusive) {
            return comparison >= 0;
        } else {
            return comparison > 0;
        }
    }

}
