package de.bwaldvogel.mongo.wire.message;

import io.netty.channel.Channel;

public class MongoGetMore extends ClientRequest {
    private long cursorId;
    private int numberToReturn;

    public MongoGetMore(Channel channel, MessageHeader header, String fullCollectionName, int numberToReturn, long cursorId) {
        super(channel, header, fullCollectionName);
        this.cursorId = cursorId;
        this.numberToReturn = numberToReturn;
    }

    public long getCursorId() {
        return cursorId;
    }

    public int getNumberToReturn() {
        return numberToReturn;
    }
}
