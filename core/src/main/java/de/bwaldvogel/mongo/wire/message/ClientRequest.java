package de.bwaldvogel.mongo.wire.message;

import de.bwaldvogel.mongo.backend.Utils;

public abstract class ClientRequest implements Message {

    private final MessageHeader header;
    private final String fullCollectionName;

    protected ClientRequest(MessageHeader header, String fullCollectionName) {
        this.header = header;
        this.fullCollectionName = fullCollectionName;
    }

    public MessageHeader getHeader() {
        return header;
    }

    @Override
    public String getDatabaseName() {
        return Utils.getDatabaseNameFromFullName(fullCollectionName);
    }

    public String getCollectionName() {
        return Utils.getCollectionNameFromFullName(fullCollectionName);
    }

    public String getFullCollectionName() {
        return fullCollectionName;
    }
}
