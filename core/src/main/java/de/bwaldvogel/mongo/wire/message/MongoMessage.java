package de.bwaldvogel.mongo.wire.message;

import de.bwaldvogel.mongo.bson.Document;
import io.netty.channel.Channel;

public class MongoMessage extends ClientRequest {

    public static final int SECTION_KIND_BODY = 0;
    public static final int SECTION_KIND_DOCUMENT_SEQUENCE = 1;

    private final Document document;
    private final int flags = 0;

    public MongoMessage(Channel channel, MessageHeader header, Document document) {
        super(channel, header, null);
        this.document = document;
    }

    public Document getDocument() {
        return document;
    }

    public int getFlags() {
        return flags;
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
