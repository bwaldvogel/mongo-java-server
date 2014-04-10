package de.bwaldvogel.mongo.backend.memory;

import io.netty.channel.Channel;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.TreeMap;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.backend.MongoBackend;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.exception.MongoServerException;
import de.bwaldvogel.mongo.exception.MongoSilentServerException;
import de.bwaldvogel.mongo.exception.NoSuchCommandException;
import de.bwaldvogel.mongo.wire.BsonConstants;
import de.bwaldvogel.mongo.wire.MongoWireProtocolHandler;
import de.bwaldvogel.mongo.wire.message.Message;
import de.bwaldvogel.mongo.wire.message.MongoDelete;
import de.bwaldvogel.mongo.wire.message.MongoInsert;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import de.bwaldvogel.mongo.wire.message.MongoUpdate;

public class MemoryBackend implements MongoBackend {

    private static int[] VERSION = new int[] { 2, 4, 0 };

    private static final Logger log = LoggerFactory.getLogger(MemoryBackend.class);

    private final TreeMap<String, MongoDatabase> databases = new TreeMap<String, MongoDatabase>();

    protected BSONObject handleAdminCommand(Channel channel, String command, BSONObject query)
            throws MongoServerException {

        if (command.equalsIgnoreCase("listdatabases")) {
            BSONObject response = new BasicBSONObject();
            List<BSONObject> dbs = new ArrayList<BSONObject>();
            for (MongoDatabase db : databases.values()) {
                BasicBSONObject dbObj = new BasicBSONObject("name", db.getDatabaseName());
                dbObj.put("empty", Boolean.valueOf(db.isEmpty()));
                dbs.add(dbObj);
            }
            response.put("databases", dbs);
            Utils.markOkay(response);
            return response;
        } else if (command.equalsIgnoreCase("replSetGetStatus")) {
            throw new MongoSilentServerException("not running with --replSet");
        } else if (command.equalsIgnoreCase("getLog")) {
            final Object argument = query.get(command);
            BSONObject response = getLog(argument == null ? null : argument.toString());
            return response;
        } else {
            throw new NoSuchCommandException(command);
        }
    }

    private String join(int[] array, char c) {
        final StringBuilder sb = new StringBuilder();
        for (int value : array) {
            if (sb.length() > 0)
                sb.append(c);
            sb.append(Integer.toString(value));
        }
        return sb.toString();
    }

    private BSONObject getLog(String argument) throws MongoServerException {
        log.debug("getLog: {}", argument);
        BSONObject response = new BasicBSONObject();
        if (argument.equals("*")) {
            response.put("names", Arrays.asList("startupWarnings"));
            Utils.markOkay(response);
        } else if (argument.equals("startupWarnings")) {
            response.put("totalLinesWritten", Integer.valueOf(0));
            response.put("log", new ArrayList<String>());
            Utils.markOkay(response);
        } else {
            throw new MongoSilentServerException("no RamLog named: " + argument);
        }
        return response;
    }

    private synchronized MongoDatabase resolveDatabase(Message message) throws MongoServerException {
        return resolveDatabase(message.getDatabaseName());
    }

    private synchronized MongoDatabase resolveDatabase(String database) throws MongoServerException {
        MongoDatabase db = databases.get(database);
        if (db == null) {
            db = new MemoryDatabase(this, database);
            log.info("created database {}", db.getDatabaseName());
            databases.put(database, db);
        }
        return db;
    }

    @Override
    public void handleClose(Channel channel) {
        for (MongoDatabase db : databases.values()) {
            db.handleClose(channel);
        }
    }

    @Override
    public BSONObject handleCommand(Channel channel, String databaseName, String command, BSONObject query)
            throws MongoServerException {

        if (command.equalsIgnoreCase("whatsmyuri")) {
            BSONObject response = new BasicBSONObject();
            InetSocketAddress remoteAddress = (InetSocketAddress) channel.remoteAddress();
            response.put("you", remoteAddress.getAddress().getHostAddress() + ":" + remoteAddress.getPort());
            Utils.markOkay(response);
            return response;
        } else if (command.equalsIgnoreCase("ismaster")) {
            BSONObject response = new BasicBSONObject("ismaster", Boolean.TRUE);
            response.put("maxBsonObjectSize", Integer.valueOf(BsonConstants.MAX_BSON_OBJECT_SIZE));
            response.put("maxMessageSizeBytes", Integer.valueOf(MongoWireProtocolHandler.MAX_MESSAGE_SIZE_BYTES));
            response.put("localTime", new Date());
            Utils.markOkay(response);
            return response;
        } else if (command.equalsIgnoreCase("buildinfo")) {
            BSONObject response = new BasicBSONObject("version", join(VERSION, '.'));
            response.put("versionArray", VERSION);
            response.put("maxBsonObjectSize", Integer.valueOf(BsonConstants.MAX_BSON_OBJECT_SIZE));
            Utils.markOkay(response);
            return response;
        }

        if (databaseName.equals("admin")) {
            return handleAdminCommand(channel, command, query);
        } else {
            MongoDatabase db = resolveDatabase(databaseName);
            return db.handleCommand(channel, command, query);
        }
    }

    @Override
    public Collection<BSONObject> getCurrentOperations(MongoQuery query) {
        // TODO
        return Collections.emptyList();
    }

    @Override
    public Iterable<BSONObject> handleQuery(MongoQuery query) throws MongoServerException {
        MongoDatabase db = resolveDatabase(query);
        return db.handleQuery(query);
    }

    @Override
    public void handleInsert(MongoInsert insert) throws MongoServerException {
        MongoDatabase db = resolveDatabase(insert);
        db.handleInsert(insert);
    }

    @Override
    public void handleDelete(MongoDelete delete) throws MongoServerException {
        MongoDatabase db = resolveDatabase(delete);
        db.handleDelete(delete);
    }

    @Override
    public void handleUpdate(MongoUpdate update) throws MongoServerException {
        MongoDatabase db = resolveDatabase(update);
        db.handleUpdate(update);
    }

    public void dropDatabase(MemoryDatabase memoryDatabase) {
        databases.remove(memoryDatabase.getDatabaseName());
    }

}
