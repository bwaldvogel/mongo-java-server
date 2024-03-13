package de.bwaldvogel.mongo.wire.message;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.wire.ReplyFlag;

public class MongoReply {
    private final MessageHeader header;
    private final List<? extends Document> documents;
    private final long cursorId;
    private int startingFrom;
    private int flags;

    public MongoReply(MessageHeader header, Document document, ReplyFlag... replyFlags) {
        this(header, List.of(document), 0, replyFlags);
    }

    public MongoReply(MessageHeader header, Iterable<? extends Document> documents, long cursorId, ReplyFlag... replyFlags) {
        this.cursorId = cursorId;
        this.header = header;
        this.documents = StreamSupport.stream(documents.spliterator(), false).collect(Collectors.toList());
        for (ReplyFlag replyFlag : replyFlags) {
            flags = replyFlag.addTo(flags);
        }
    }

    public MessageHeader getHeader() {
        return header;
    }

    public List<Document> getDocuments() {
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
