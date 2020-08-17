package de.bwaldvogel.mongo.wire.message;

import io.netty.channel.Channel;

public class MongoGetMore extends ClientRequest {
    private final long cursorId;
    private final int numberToReturn;

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

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName()).append("(");
        sb.append("header: ").append(getHeader());
        sb.append(", collection: ").append(getFullCollectionName());
        sb.append(", cursorId: ").append(cursorId);
        sb.append(", numberToReturn: ").append(numberToReturn);
        sb.append(")");
        return sb.toString();
    }

}
