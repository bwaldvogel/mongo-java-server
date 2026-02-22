package de.bwaldvogel.mongo.backend;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum Command {
    // User
    AGGREGATE("aggregate"),
    COUNT("count"),
    DISTINCT("distinct"),

    // Query and write operation
    DELETE("delete"),
    FIND("find"),
    FIND_AND_MODIFY("findAndModify"),
    GET_LAST_ERROR("getLastError"), // removed in 5.1
    GET_MORE("getMore"),
    INSERT("insert"),
    RESET_ERROR("resetError"),
    UPDATE("update"),

    // Replication
    IS_MASTER("isMaster"),
    REPL_SET_GET_STATUS("replSetGetStatus"),

    // Session
    END_SESSIONS("endSessions"),

    //  Administration
    CREATE("create"),
    CREATE_INDEXES("createIndexes"),
    DROP("drop"),
    DROP_DATABASE("dropDatabase"),
    DROP_INDEXES("dropIndexes"),
    KILL_CURSORS("killCursors"),
    LIST_COLLECTIONS("listCollections"),
    LIST_DATABASES("listDatabases"),
    LIST_INDEXES("listIndexes"),
    RENAME_COLLECTION("renameCollection"),

    // Diagnostic
    BUILD_INFO("buildInfo"),
    COLL_STATS("collStats"),
    CONNECTION_STATUS("connectionStatus"),
    DB_STATS("dbStats"),
    GET_CMD_LINE_OPTS("getCmdLineOpts"),
    GET_LOG("getLog"),
    HOST_INFO("hostInfo"),
    PING("ping"),
    SERVER_STATUS("serverStatus"),
    VALIDATE("validate"),
    WHATS_MY_URI("whatsmyuri"),

    // Monitoring
    GET_FREE_MONITORING_STATUS("getFreeMonitoringStatus"),

    // internal use
    TRIGGER_INTERNAL_EXCEPTION("triggerInternalException"),
    UNKNOWN("unknown");

    private final String name;
    private static final Map<String, Command> LOOKUP;

    static {
        LOOKUP = Arrays.stream(Command.values())
            .collect(Collectors.toMap(
                c -> c.name.toLowerCase(),
                Function.identity()
            ));
    }

    Command(String name) {
        this.name = name;
    }

    String getName() {
        return name;
    }

    public static Command parseString(String commandName) {
        return LOOKUP.getOrDefault(commandName.toLowerCase(), UNKNOWN);
    }
}
