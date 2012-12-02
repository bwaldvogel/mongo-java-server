package de.bwaldvogel.mongo.wire.message;

public abstract class ClientRequest implements Message {

    private final MessageHeader header;
    private final String fullCollectionName;
    private int clientId;

    public ClientRequest(int clientId , MessageHeader header , String fullCollectionName) {
        this.clientId = clientId;
        this.header = header;
        this.fullCollectionName = fullCollectionName;
    }

    public int getClientId() {
        return clientId;
    }

    public MessageHeader getHeader() {
        return header;
    }

    public String getDatabaseName() {
        int index = fullCollectionName.indexOf( '.' );
        return fullCollectionName.substring( 0, index );
    }

    public String getCollectionName() {
        int index = fullCollectionName.indexOf( '.' );
        return fullCollectionName.substring( index + 1 );
    }

    public String getFullCollectionName() {
        return fullCollectionName;
    }
}
