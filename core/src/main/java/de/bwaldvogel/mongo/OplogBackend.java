package de.bwaldvogel.mongo;

import de.bwaldvogel.mongo.bson.Document;
import io.netty.channel.Channel;

public interface OplogBackend {
    void handleOplog(Channel channel, String databaseName, String command, Document query);
}
