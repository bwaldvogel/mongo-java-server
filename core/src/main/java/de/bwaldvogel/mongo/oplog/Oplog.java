package de.bwaldvogel.mongo.oplog;

import de.bwaldvogel.mongo.bson.Document;
import io.netty.channel.Channel;

public interface Oplog {

    void handleCommand(String databaseName, String command, Document query);
    void handleInsert(String databaseName, Document query);
    void handleUpdate(String databaseName, Document query);
    void handleDelete(String databaseName, Document query);
}
