package de.bwaldvogel.mongo.wire.message;

import java.util.Collections;
import java.util.List;

import org.bson.BSONObject;

import de.bwaldvogel.mongo.wire.ReplyFlag;

public class MongoReply {
    private final MessageHeader header;
    private final List<? extends BSONObject> documents;
    private long cursorId;
    private int startingFrom;
    private int flags;

    public MongoReply(MessageHeader header, BSONObject document, ReplyFlag... replyFlags) {
        this(header, Collections.singletonList(document), replyFlags);
    }

    public MongoReply(MessageHeader header, List<? extends BSONObject> documents, ReplyFlag... replyFlags) {
        this.header = header;
        this.documents = documents;
        for (ReplyFlag replyFlag : replyFlags) {
            flags = replyFlag.addTo(flags);
        }
    }

    public MessageHeader getHeader() {
        return header;
    }

    public List<BSONObject> getDocuments() {
        return Collections.unmodifiableList(documents);
    }

    public long getCursorId() {
        return cursorId;
    }

    public int getStartingFrom() {
        return startingFrom;
    }

    public int getFlags() {
        return flags;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName());
        sb.append("(");
        sb.append("documents: ").append(getDocuments());
        sb.append(")");
        return sb.toString();
    }
}
