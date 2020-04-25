package de.bwaldvogel.mongo.oplog;

import java.time.Clock;

import de.bwaldvogel.mongo.bson.Document;
import io.netty.channel.Channel;

public abstract class AbstractOplog implements Oplog {

    protected final Clock clock;

    protected AbstractOplog(Clock clock) {
        this.clock = clock;
    }

    @Override
    public final void handleCommand(String databaseName, String command, Document query) {
        switch (command) {
            case "insert":
                handleInsert(databaseName, query);
                break;
            case "update":
                handleUpdate(databaseName, query);
                break;
            case "delete":
                handleDelete(databaseName, query);
                break;
        }
    }
}
