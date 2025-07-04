package de.bwaldvogel.mongo.backend;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.MongoVersion;
import de.bwaldvogel.mongo.ServerVersion;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerException;
import de.bwaldvogel.mongo.exception.MongoSilentServerException;
import de.bwaldvogel.mongo.exception.NamespaceExistsException;
import de.bwaldvogel.mongo.exception.NoReplicationEnabledException;
import de.bwaldvogel.mongo.exception.NoSuchCommandException;
import de.bwaldvogel.mongo.oplog.CollectionBackedOplog;
import de.bwaldvogel.mongo.oplog.NoopOplog;
import de.bwaldvogel.mongo.oplog.Oplog;
import de.bwaldvogel.mongo.wire.BsonConstants;
import de.bwaldvogel.mongo.wire.MongoWireProtocolHandler;
import de.bwaldvogel.mongo.wire.message.Message;
import de.bwaldvogel.mongo.wire.message.MongoMessage;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import io.netty.channel.Channel;

public abstract class AbstractMongoBackend implements MongoBackend {

    private static final Logger log = LoggerFactory.getLogger(AbstractMongoBackend.class);

    protected static final String OPLOG_COLLECTION_NAME = "oplog.rs";

    static final String ADMIN_DB_NAME = "admin";

    private final Map<String, MongoDatabase> databases = new ConcurrentHashMap<>();

    private MongoVersion version = ServerVersion.MONGO_5_0;

    private final Clock clock;
    private final Instant started;

    private final CursorRegistry cursorRegistry = new CursorRegistry();

    protected Oplog oplog = NoopOplog.get();

    protected AbstractMongoBackend() {
        this(defaultClock());
    }

    protected AbstractMongoBackend(Clock clock) {
        this.started = Instant.now(clock);
        this.clock = clock;
    }

    protected static Clock defaultClock() {
        return Clock.systemDefaultZone();
    }

    private MongoDatabase resolveDatabase(Message message) {
        return resolveDatabase(message.getDatabaseName());
    }

    @Override
    public MongoDatabase resolveDatabase(String databaseName) {
        return databases.computeIfAbsent(databaseName, name -> {
            MongoDatabase database = openOrCreateDatabase(databaseName);
            log.info("created database {}", database.getDatabaseName());
            return database;
        });
    }

    @Override
    public Document getServerStatus() {
        Document serverStatus = new Document();
        try {
            serverStatus.put("host", InetAddress.getLocalHost().getHostName());
        } catch (UnknownHostException e) {
            throw new MongoServerException("failed to get hostname", e);
        }
        serverStatus.put("version", version.toVersionString());
        serverStatus.put("process", "java");
        serverStatus.put("pid", getProcessId());

        Duration uptime = Duration.between(started, Instant.now(clock));
        serverStatus.put("uptime", Integer.valueOf(Math.toIntExact(uptime.getSeconds())));
        serverStatus.put("uptimeMillis", Long.valueOf(uptime.toMillis()));
        serverStatus.put("localTime", Instant.now(getClock()));

        Document connections = new Document();
        connections.put("current", Integer.valueOf(1));

        serverStatus.put("connections", connections);

        Document metrics = new Document();
        Document cursorMetrics = new Document();
        cursorMetrics.put("timedOut", 0L);

        Document openCursors = new Document();
        openCursors.put("noTimeout", 0L);
        openCursors.put("pinned", 0L);
        openCursors.put("total", (long) cursorRegistry.size());
        cursorMetrics.put("open", openCursors);

        metrics.put("cursor", cursorMetrics);
        serverStatus.put("metrics", metrics);

        Utils.markOkay(serverStatus);

        return serverStatus;
    }

    private Integer getProcessId() {
        String runtimeName = ManagementFactory.getRuntimeMXBean().getName();
        if (runtimeName.contains("@")) {
            return Integer.valueOf(runtimeName.substring(0, runtimeName.indexOf('@')));
        }
        return Integer.valueOf(0);
    }

    private Document getLog(String argument) {
        log.debug("getLog: {}", argument);
        Document response = new Document();
        switch (argument) {
            case "*":
                response.put("names", List.of("startupWarnings"));
                Utils.markOkay(response);
                break;
            case "startupWarnings":
                response.put("totalLinesWritten", Integer.valueOf(0));
                response.put("log", new ArrayList<String>());
                Utils.markOkay(response);
                break;
            default:
                throw new MongoSilentServerException("no RamLog named: " + argument);
        }
        return response;
    }

    private Document handleAdminCommand(String command, Document query) {
        if (command.equalsIgnoreCase("listdatabases")) {
            List<Document> databases = listDatabaseNames().stream()
                .sorted()
                .map(databaseName -> {
                    MongoDatabase database = openOrCreateDatabase(databaseName);
                    Document dbObj = new Document("name", database.getDatabaseName());
                    dbObj.put("empty", Boolean.valueOf(database.isEmpty()));
                    return dbObj;
                })
                .collect(Collectors.toList());
            Document response = new Document();
            response.put("databases", databases);
            Utils.markOkay(response);
            return response;
        } else if (command.equalsIgnoreCase("find")) {
            String collectionName = (String) query.get(command);
            if (collectionName.equals("$cmd.sys.inprog")) {
                return Utils.firstBatchCursorResponse(collectionName, new Document("inprog", Collections.emptyList()));
            } else {
                throw new NoSuchCommandException(new Document(command, collectionName).toString());
            }
        } else if (command.equalsIgnoreCase("replSetGetStatus")) {
            throw new NoReplicationEnabledException();
        } else if (command.equalsIgnoreCase("getLog")) {
            final Object argument = query.get(command);
            return getLog(argument == null ? null : argument.toString());
        } else if (command.equalsIgnoreCase("renameCollection")) {
            return handleRenameCollection(command, query);
        } else if (command.equalsIgnoreCase("getLastError")) {
            log.debug("getLastError on admin database");
            return successResponse();
        } else if (command.equalsIgnoreCase("connectionStatus")) {
            Document response = new Document();
            response.append("authInfo", new Document()
                .append("authenticatedUsers", Collections.emptyList())
                .append("authenticatedUserRoles", Collections.emptyList())
            );
            Utils.markOkay(response);
            return response;
        } else if (command.equalsIgnoreCase("hostInfo")) {
            return handleHostInfo();
        } else if (command.equalsIgnoreCase("getCmdLineOpts")) {
            return handleGetCmdLineOpts();
        } else if (command.equalsIgnoreCase("getFreeMonitoringStatus")) {
            return handleGetFreeMonitoringStatus();
        } else if (command.equalsIgnoreCase("endSessions")) {
            log.debug("endSessions on admin database");
            return successResponse();
        } else {
            throw new NoSuchCommandException(command);
        }
    }

    private static Document successResponse() {
        Document response = new Document();
        Utils.markOkay(response);
        return response;
    }

    private Document handleHostInfo() {
        Document response = new Document();
        String osName = System.getProperty("os.name");
        String osVersion = System.getProperty("os.version");
        response.append("os", new Document()
            .append("type", osName)
            .append("version", osVersion)
        );
        response.append("system", new Document()
            .append("currentTime", Instant.now())
            .append("hostname", Utils.getHostName())
        );
        response.append("extra", new Document()
            .append("versionString", osName + " " + osVersion)
            .append("kernelVersion", osVersion));
        Utils.markOkay(response);
        return response;
    }

    private Document handleGetCmdLineOpts() {
        Document response = new Document();
        response.append("argv", Collections.emptyList());
        response.append("parsed", new Document());
        Utils.markOkay(response);
        return response;
    }

    private Document handleGetFreeMonitoringStatus() {
        Document response = new Document();
        response.append("state", "disabled");
        response.append("debug", Map.of("state", "undecided"));
        response.append("message", "Free monitoring is deprecated, refer to 'debug' field for actual status");
        Utils.markOkay(response);
        return response;
    }

    @VisibleForExternalBackends
    protected Set<String> listDatabaseNames() {
        return databases.keySet();
    }

    private Document handleRenameCollection(String command, Document query) {
        final String oldNamespace = query.get(command).toString();
        final String newNamespace = query.get("to").toString();
        boolean dropTarget = Utils.isTrue(query.get("dropTarget"));
        Document response = new Document();

        if (!oldNamespace.equals(newNamespace)) {
            MongoCollection<?> oldCollection = resolveCollection(oldNamespace);
            if (oldCollection == null) {
                throw new MongoServerException("source namespace does not exist");
            }

            String newDatabaseName = Utils.getDatabaseNameFromFullName(newNamespace);
            String newCollectionName = Utils.getCollectionNameFromFullName(newNamespace);
            MongoDatabase oldDatabase = resolveDatabase(oldCollection.getDatabaseName());
            MongoDatabase newDatabase = resolveDatabase(newDatabaseName);
            MongoCollection<?> newCollection = newDatabase.resolveCollection(newCollectionName, false);
            if (newCollection != null) {
                if (dropTarget) {
                    newDatabase.dropCollection(newCollectionName, oplog);
                } else {
                    throw new NamespaceExistsException("target namespace exists");
                }
            }

            newDatabase.moveCollection(oldDatabase, oldCollection, newCollectionName);
        }
        Utils.markOkay(response);
        return response;
    }

    private MongoCollection<?> resolveCollection(String namespace) {
        String databaseName = Utils.getDatabaseNameFromFullName(namespace);
        String collectionName = Utils.getCollectionNameFromFullName(namespace);

        MongoDatabase database = databases.get(databaseName);
        if (database == null) {
            return null;
        }

        return database.resolveCollection(collectionName, false);
    }

    protected abstract MongoDatabase openOrCreateDatabase(String databaseName);

    @Override
    public Document handleCommand(Channel channel, String databaseName, String command, Document query) {
        if (command.equalsIgnoreCase("whatsmyuri")) {
            Document response = new Document();
            InetSocketAddress remoteAddress = (InetSocketAddress) channel.remoteAddress();
            response.put("you", remoteAddress.getAddress().getHostAddress() + ":" + remoteAddress.getPort());
            Utils.markOkay(response);
            return response;
        } else if (command.equalsIgnoreCase("ismaster")) {
            Document response = new Document("ismaster", Boolean.TRUE);
            response.put("maxBsonObjectSize", Integer.valueOf(BsonConstants.MAX_BSON_OBJECT_SIZE));
            response.put("maxWriteBatchSize", Integer.valueOf(MongoWireProtocolHandler.MAX_WRITE_BATCH_SIZE));
            response.put("maxMessageSizeBytes", Integer.valueOf(MongoWireProtocolHandler.MAX_MESSAGE_SIZE_BYTES));
            response.put("maxWireVersion", Integer.valueOf(version.getWireVersion()));
            response.put("minWireVersion", Integer.valueOf(0));
            response.put("localTime", Instant.now(clock));
            Utils.markOkay(response);
            return response;
        } else if (command.equalsIgnoreCase("buildinfo")) {
            Document response = new Document("version", version.toVersionString());
            response.put("versionArray", version.getVersionArray());
            response.put("maxBsonObjectSize", Integer.valueOf(BsonConstants.MAX_BSON_OBJECT_SIZE));
            Utils.markOkay(response);
            return response;
        } else if (command.equalsIgnoreCase("dropDatabase")) {
            return handleDropDatabase(databaseName);
        } else if (command.equalsIgnoreCase("getMore")) {
            return handleGetMore(databaseName, command, query);
        } else if (command.equalsIgnoreCase("killCursors")) {
            return handleKillCursors(query);
        } else if (command.equalsIgnoreCase("ping")) {
            return successResponse();
        } else if (command.equalsIgnoreCase("serverStatus")) {
            return getServerStatus();
        } else if (databaseName.equals(ADMIN_DB_NAME)) {
            return handleAdminCommand(command, query);
        }

        MongoDatabase mongoDatabase = resolveDatabase(databaseName);
        return mongoDatabase.handleCommand(channel, command, query, this::resolveDatabase, oplog);
    }

    @Override
    public Collection<Document> getCurrentOperations(MongoQuery query) {
        // TODO
        return Collections.emptyList();
    }

    @Override
    public QueryResult handleQuery(MongoQuery query) {
        return resolveDatabase(query).handleQuery(query);
    }

    @Override
    public void closeCursors(List<Long> cursorIds) {
        cursorIds.forEach(cursorRegistry::remove);
    }

    protected Document handleKillCursors(Document query) {
        List<Long> cursorIds = (List<Long>) query.get("cursors");
        List<Long> cursorsKilled = new ArrayList<>();
        List<Long> cursorsNotFound = new ArrayList<>();
        for (Long cursorId : cursorIds) {
            if (cursorRegistry.remove(cursorId)) {
                log.info("Killed cursor {}", cursorId);
                cursorsKilled.add(cursorId);
            } else {
                log.info("Cursor {} not found", cursorId);
                cursorsNotFound.add(cursorId);
            }
        }
        Document response = new Document();
        response.put("cursorsKilled", cursorsKilled);
        response.put("cursorsNotFound", cursorsNotFound);
        Utils.markOkay(response);
        return response;
    }

    protected Document handleGetMore(String databaseName, String command, Document query) {
        MongoDatabase mongoDatabase = resolveDatabase(databaseName);
        String collectionName = (String) query.get("collection");
        long cursorId = ((Number) query.get(command)).longValue();
        int batchSize = ((Number) query.getOrDefault("batchSize", 0)).intValue();
        QueryResult queryResult = handleGetMore(cursorId, batchSize);
        List<Document> nextBatch = queryResult.collectDocuments();
        String fullCollectionName = databaseName + "." + collectionName;
        return Utils.nextBatchCursorResponse(fullCollectionName, nextBatch, queryResult.getCursorId());
    }

    private QueryResult handleGetMore(long cursorId, int numberToReturn) {
        Cursor cursor = cursorRegistry.getCursor(cursorId);
        List<Document> documents = cursor.takeDocuments(numberToReturn);
        if (cursor.isEmpty()) {
            log.debug("Removing empty {}", cursor);
            cursorRegistry.remove(cursor);
        }
        return new QueryResult(documents, cursor.isEmpty() ? EmptyCursor.get().getId() : cursorId);
    }

    protected Document handleDropDatabase(String databaseName) {
        dropDatabase(databaseName);
        Document response = new Document("dropped", databaseName);
        Utils.markOkay(response);
        return response;
    }

    @Override
    public Document handleMessage(MongoMessage message) {
        Channel channel = message.getChannel();
        String databaseName = message.getDatabaseName();
        Document query = message.getDocument();
        String command = query.keySet().iterator().next();
        return handleCommand(channel, databaseName, command, query);
    }

    @Override
    public void dropDatabase(String databaseName) {
        MongoDatabase removedDatabase = databases.remove(databaseName);
        if (removedDatabase != null) {
            removedDatabase.drop(oplog);
        }
    }

    @Override
    public void handleClose(Channel channel) {
        for (MongoDatabase db : databases.values()) {
            db.handleClose(channel);
        }
    }

    @Override
    public void close() {
        log.info("closing {}", this);
        databases.clear();
    }

    @Override
    public MongoBackend version(MongoVersion version) {
        this.version = version;
        return this;
    }

    @Override
    public Clock getClock() {
        return clock;
    }

    @Override
    public void disableOplog() {
        oplog = NoopOplog.get();
    }

    @Override
    public void enableOplog() {
        oplog = createOplog();
    }

    protected Oplog createOplog() {
        MongoDatabase localDatabase = resolveDatabase("local");
        MongoCollection<Document> collection = (MongoCollection<Document>) localDatabase.resolveCollection(OPLOG_COLLECTION_NAME, false);
        if (collection == null) {
            collection = (MongoCollection<Document>) localDatabase.createCollectionOrThrowIfExists(OPLOG_COLLECTION_NAME);
        }
        return new CollectionBackedOplog(this, collection, cursorRegistry);
    }

    protected CursorRegistry getCursorRegistry() {
        return cursorRegistry;
    }

}
