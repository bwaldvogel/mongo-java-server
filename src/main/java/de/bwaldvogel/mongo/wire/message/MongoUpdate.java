package de.bwaldvogel.mongo.wire.message;

import org.bson.BSONObject;
import org.jboss.netty.channel.Channel;

public class MongoUpdate extends ClientRequest {

    private BSONObject selector;
    private BSONObject update;
    private boolean upsert;
    private boolean multi;

    public MongoUpdate(Channel channel, MessageHeader header, String fullCollectionName, BSONObject selector,
            BSONObject update, boolean upsert, boolean multi) {
        super(channel, header, fullCollectionName);
        this.selector = selector;
        this.update = update;
        this.upsert = upsert;
        this.multi = multi;
    }

    public boolean isUpsert() {
        return upsert;
    }

    public boolean isMulti() {
        return multi;
    }

    public BSONObject getSelector() {
        return selector;
    }

    public BSONObject getUpdate() {
        return update;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName()).append("(");
        sb.append("header: ").append(getHeader());
        sb.append(", collection: ").append(getFullCollectionName());
        sb.append(", selector: ").append(selector);
        sb.append(", update: ").append(update);
        sb.append(")");
        return sb.toString();
    }

}
