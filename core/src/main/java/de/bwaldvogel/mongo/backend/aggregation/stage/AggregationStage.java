package de.bwaldvogel.mongo.backend.aggregation.stage;

import java.util.stream.Stream;

import de.bwaldvogel.mongo.bson.Document;

public interface AggregationStage {

    String name();

    Stream<Document> apply(Stream<Document> stream);

    default boolean isModifying() {
        return false;
    }
}
