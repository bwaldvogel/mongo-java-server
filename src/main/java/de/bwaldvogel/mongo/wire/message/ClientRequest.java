package de.bwaldvogel.mongo.wire.message;

import org.jboss.netty.channel.Channel;

public abstract class ClientRequest implements Message {

    private final MessageHeader header;
    private final String fullCollectionName;
    private Channel channel;

    public ClientRequest(Channel channel , MessageHeader header , String fullCollectionName) {
        this.channel = channel;
        this.header = header;
        this.fullCollectionName = fullCollectionName;
    }

    public int getClientId() {
        return channel.getId().intValue();
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
