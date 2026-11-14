package de.bwaldvogel.mongo.wire.message;

import java.util.Set;

import de.bwaldvogel.mongo.bson.Document;
import io.netty.channel.Channel;

public class MongoMessage extends ClientRequest {

    public static final int SECTION_KIND_BODY = 0;
    public static final int SECTION_KIND_DOCUMENT_SEQUENCE = 1;

    private final Document document;
    private final Set<MessageFlag> flags;

    public MongoMessage(Channel channel, MessageHeader header, Document document) {
        this(channel, header, document, Set.of());
    }

    public MongoMessage(Channel channel, MessageHeader header, Document document, Set<MessageFlag> flags) {
        super(channel, header, null);
        this.document = document;
        this.flags = Set.copyOf(flags);
    }

    public Document getDocument() {
        return document;
    }

    public int getFlags() {
        int flagBits = 0;
        for (MessageFlag flag : flags) {
            flagBits |= flag.getValue();
        }
        return flagBits;
    }

    /**
     * Whether the client set the moreToCome flag: it does not wait for a reply, as for an unacknowledged (w=0) write.
     */
    public boolean isMoreToCome() {
        return flags.contains(MessageFlag.MORE_TO_COME);
    }

    @Override
    public String getDatabaseName() {
        return (String) document.get("$db");
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName()).append("(");
        sb.append("header: ").append(getHeader());
        sb.append(", collection: ").append(getFullCollectionName());
        sb.append(", document: ").append(document);
        sb.append(")");
        return sb.toString();
    }
}
