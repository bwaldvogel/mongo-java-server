package de.bwaldvogel.mongo.backend;

import java.time.Clock;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.oplog.CollectionBackedOplog;
import de.bwaldvogel.mongo.oplog.Oplog;
import io.netty.channel.Channel;

public class Session {
    public final UUID id;
    private final ConcurrentHashMap<String, MongoDatabase> databases = new ConcurrentHashMap<>();
    private Oplog oplog;

    public Session(UUID id, MongoDatabase adminDatabase, Clock clock) {
        this.id = id;
        MongoCollection<Document> oplogCollection = (MongoCollection<Document>)
            adminDatabase.createCollectionOrThrowIfExists("oplog.rs", CollectionOptions.withDefaults());
        oplog = new CollectionBackedOplog(clock, oplogCollection, new CursorRegistry());
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        Session other = (Session) obj;
        return id.equals(other.id);
    }

    public Document handleCommand(MongoDatabase database, Channel channel, String command, Document query) {
        MongoDatabase sessionDatabase;
        if (databases.containsKey(database.getDatabaseName())) {
            sessionDatabase = databases.get(database.getDatabaseName());
        } else {
//            sessionDatabase = database.deepClone();
            sessionDatabase = database;
            databases.put(sessionDatabase.getDatabaseName(), sessionDatabase);
        }
        return sessionDatabase.handleCommand(channel, command, query, oplog);
    }

}
