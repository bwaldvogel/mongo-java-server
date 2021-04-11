package de.bwaldvogel.mongo.backend.aggregation.stage;

import java.util.Collections;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.CollectionOptions;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.oplog.NoopOplog;

public class OutStage implements AggregationStage {

    private static final Logger log = LoggerFactory.getLogger(OutStage.class);

    private final MongoDatabase database;
    private final String collectionName;

    public OutStage(MongoDatabase database, Object collectionName) {
        this.database = database;
        this.collectionName = (String) collectionName;
        if (this.collectionName.contains("$")) {
            throw new MongoServerError(17385, "Can't $out to special collection: " + this.collectionName);
        }
    }

    @Override
    public Stream<Document> apply(Stream<Document> stream) {
        String tempCollectionName = "_tmp" + System.currentTimeMillis() + "_" + this.collectionName;
        MongoCollection<?> tempCollection = database.createCollectionOrThrowIfExists(tempCollectionName, CollectionOptions.withDefaults());
        stream.forEach(document -> tempCollection.insertDocuments(Collections.singletonList(document)));
        MongoCollection<?> existingCollection = database.resolveCollection(this.collectionName, false);
        if (existingCollection != null) {
            log.info("Dropping existing collection {}", existingCollection);
            database.dropCollection(this.collectionName, NoopOplog.get());
        }
        database.moveCollection(database, tempCollection, this.collectionName);
        return Stream.empty();
    }

}
