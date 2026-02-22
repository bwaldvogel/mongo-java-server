package de.bwaldvogel.mongo.backend;

import static de.bwaldvogel.mongo.backend.Command.BUILD_INFO;
import static de.bwaldvogel.mongo.backend.Command.COLL_STATS;
import static de.bwaldvogel.mongo.backend.Command.COUNT;
import static de.bwaldvogel.mongo.backend.Command.DB_STATS;
import static de.bwaldvogel.mongo.backend.Command.DISTINCT;
import static de.bwaldvogel.mongo.backend.Command.FIND;
import static de.bwaldvogel.mongo.backend.Command.GET_LAST_ERROR;
import static de.bwaldvogel.mongo.backend.Command.GET_MORE;
import static de.bwaldvogel.mongo.backend.Command.IS_MASTER;
import static de.bwaldvogel.mongo.backend.Command.LIST_DATABASES;
import static de.bwaldvogel.mongo.backend.Command.SERVER_STATUS;

import java.time.Clock;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.MongoVersion;
import de.bwaldvogel.mongo.backend.aggregation.Aggregation;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerException;
import de.bwaldvogel.mongo.exception.NoSuchCommandException;
import de.bwaldvogel.mongo.wire.message.MongoMessage;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import io.netty.channel.Channel;

public class ReadOnlyProxy implements MongoBackend {

    private static final Set<Command> allowedCommands = Set.of(
        IS_MASTER,
        FIND,
        LIST_DATABASES,
        COUNT,
        DB_STATS,
        DISTINCT,
        COLL_STATS,
        SERVER_STATUS,
        BUILD_INFO,
        GET_LAST_ERROR,
        GET_MORE
    );

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
    public Document handleCommand(Channel channel, String database, DatabaseCommand command, Document query) {
        if (isAllowed(command.getCommand(), query)) {
            return backend.handleCommand(channel, database, command, query);
        }
        throw new NoSuchCommandException(command.getQueryValue());
    }

    private static boolean isAllowed(Command command, Document query) {
        if (allowedCommands.contains(command)) {
            return true;
        }

        if (command == Command.AGGREGATE) {
            List<Document> pipeline = Aggregation.parse(query.get("pipeline"));
            Aggregation aggregation = Aggregation.fromPipeline(pipeline, null, null, null, null);
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
        if (isAllowed(Command.parseString(command), document)) {
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
    public MongoBackend version(MongoVersion version) {
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
