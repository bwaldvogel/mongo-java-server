package de.bwaldvogel.mongo.backend.aggregation.stage;

import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.IllegalOperationException;
import de.bwaldvogel.mongo.oplog.NoopOplog;

public class OutStage extends TerminalStage {

    private static final Logger log = LoggerFactory.getLogger(OutStage.class);

    private final MongoDatabase database;
    private final String collectionName;

    public OutStage(MongoDatabase database, Object collectionName) {
        this.database = database;
        this.collectionName = (String) collectionName;
    }

    @Override
    public String name() {
        return "$out";
    }

    @Override
    public void applyLast(Stream<Document> stream) {
        if (this.collectionName.contains("$")) {
            throw new IllegalOperationException("error with target namespace: Invalid collection name: " + this.collectionName);
        }
        String tempCollectionName = "_tmp" + System.currentTimeMillis() + "_" + this.collectionName;
        MongoCollection<?> tempCollection = database.createCollectionOrThrowIfExists(tempCollectionName);
        stream.forEach(tempCollection::addDocument);
        MongoCollection<?> existingCollection = database.resolveCollection(this.collectionName, false);
        if (existingCollection != null) {
            log.info("Dropping existing collection {}", existingCollection);
            database.dropCollection(this.collectionName, NoopOplog.get());
        }
        database.moveCollection(database, tempCollection, this.collectionName);
    }

    @Override
    public boolean isModifying() {
        return true;
    }
}
