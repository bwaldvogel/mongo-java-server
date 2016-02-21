package de.bwaldvogel.mongo.bson;

public class BsonTimestamp implements Bson {

    private static final long serialVersionUID = 1L;

    private final long timestamp;

    public BsonTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
