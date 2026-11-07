package de.bwaldvogel.mongo.wire.message;

import de.bwaldvogel.mongo.bson.Document;
import io.netty.channel.Channel;

public class MongoMessage extends ClientRequest {

    public static final int SECTION_KIND_BODY = 0;
    public static final int SECTION_KIND_DOCUMENT_SEQUENCE = 1;

    private final Document document;
    private final int flags = 0;
    private final boolean moreToCome;

    public MongoMessage(Channel channel, MessageHeader header, Document document) {
        this(channel, header, document, false);
    }

    public MongoMessage(Channel channel, MessageHeader header, Document document, boolean moreToCome) {
        super(channel, header, null);
        this.document = document;
        this.moreToCome = moreToCome;
    }

    public Document getDocument() {
        return document;
    }

    public int getFlags() {
        return flags;
    }

    /**
     * Whether the client set the moreToCome flag: it does not wait for a reply, as for an unacknowledged (w=0) write.
     */
    public boolean isMoreToCome() {
        return moreToCome;
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
