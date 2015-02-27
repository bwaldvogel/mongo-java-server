package de.bwaldvogel.mongo.wire.message;

import io.netty.channel.Channel;

public abstract class ClientRequest implements Message {

    private final MessageHeader header;
    private final String fullCollectionName;
    private Channel channel;

    public ClientRequest(Channel channel, MessageHeader header, String fullCollectionName) {
        this.channel = channel;
        this.header = header;
        this.fullCollectionName = fullCollectionName;
    }

    public Channel getChannel() {
        return channel;
    }

    public MessageHeader getHeader() {
        return header;
    }

    @Override
    public String getDatabaseName() {
        int index = fullCollectionName.indexOf('.');
        return fullCollectionName.substring(0, index);
    }

    public String getCollectionName() {
        int index = fullCollectionName.indexOf('.');
        return fullCollectionName.substring(index + 1);
    }

    public String getFullCollectionName() {
        return fullCollectionName;
    }
}
