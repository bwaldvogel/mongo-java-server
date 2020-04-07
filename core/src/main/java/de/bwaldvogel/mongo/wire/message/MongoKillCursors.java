package de.bwaldvogel.mongo.wire.message;

import java.util.List;

import io.netty.channel.Channel;

public class MongoKillCursors extends ClientRequest {
    private List<Long> cursorIds;

    private MongoKillCursors(Channel channel, MessageHeader header, String fullCollectionName) {
        super(channel, header, fullCollectionName);
    }

    public MongoKillCursors(Channel channel, MessageHeader header, List<Long> cursorIds) {
        this(channel, header, "");
        this.cursorIds = cursorIds;
    }

    public List<Long> getCursorIds() {
        return cursorIds;
    }
}
