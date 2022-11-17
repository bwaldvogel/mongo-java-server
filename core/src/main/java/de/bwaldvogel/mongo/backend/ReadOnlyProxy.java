package de.bwaldvogel.mongo.backend;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.ServerVersion;
import de.bwaldvogel.mongo.backend.aggregation.Aggregation;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerException;
import de.bwaldvogel.mongo.exception.NoSuchCommandException;
import de.bwaldvogel.mongo.wire.message.MongoMessage;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import io.netty.channel.Channel;

public class ReadOnlyProxy implements MongoBackend {

    private static final Set<String> allowedCommands = new HashSet<>(Arrays.asList(
        "ismaster",
        "find",
        "listdatabases",
        "count",
        "dbstats",
        "distinct",
        "collstats",
        "serverstatus",
        "buildinfo",
        "getlasterror",
        "getmore"
    ));

    private final MongoBackend backend;

    public ReadOnlyProxy(MongoBackend backend) {
        this.backend = backend;
    }

    public static class ReadOnlyException extends MongoServerException {

        private static final long serialVersionUID = 1L;

        ReadOnlyException(String message) {
            super(message);
        }

    }

    @Override
    public void handleClose(Channel channel) {
        backend.handleClose(channel);
    }

    @Override
    public Document handleCommand(Channel channel, String database, String command, Document query) {
        if (isAllowed(command, query)) {
            return backend.handleCommand(channel, database, command, query);
        }
        throw new NoSuchCommandException(command);
    }

    private static boolean isAllowed(String command, Document query) {
        if (allowedCommands.contains(command.toLowerCase())) {
            return true;
        }

        if (command.equalsIgnoreCase("aggregate")) {
            List<Document> pipeline = Aggregation.parse(query.get("pipeline"));
            Aggregation aggregation = Aggregation.fromPipeline(pipeline, null, null, null);
            if (aggregation.isModifying()) {
                throw new MongoServerException("Aggregation contains a modifying stage and is therefore not allowed in read-only mode");
            }
            return true;
        }

        return false;
    }

    @Override
    public Document handleMessage(MongoMessage message) {
        Document document = message.getDocument();
        String command = document.keySet().iterator().next().toLowerCase();
        if (isAllowed(command, document)) {
            return backend.handleMessage(message);
        }
        throw new NoSuchCommandException(command);
    }

    @Override
    public Collection<Document> getCurrentOperations(MongoQuery query) {
        return backend.getCurrentOperations(query);
    }

    @Override
    public QueryResult handleQuery(MongoQuery query) {
        return backend.handleQuery(query);
    }

    @Override
    public void dropDatabase(String database) {
        throw new ReadOnlyException("dropping of databases is not allowed");
    }

    @Override
    public MongoBackend version(ServerVersion version) {
        throw new ReadOnlyException("not supported");
    }

    @Override
    public MongoDatabase resolveDatabase(String database) {
        throw new ReadOnlyException("resolveDatabase not allowed");
    }

    @Override
    public Document getServerStatus() {
        return backend.getServerStatus();
    }

    @Override
    public void close() {
        backend.close();
    }

    @Override
    public Clock getClock() {
        return backend.getClock();
    }

    @Override
    public void enableOplog() {
    }

    @Override
    public void disableOplog() {
    }

    @Override
    public void closeCursors(List<Long> cursorIds) {
        backend.closeCursors(cursorIds);
    }
}
