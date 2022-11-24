package de.bwaldvogel.mongo.backend.aggregation.stage;

import java.util.stream.Stream;

import de.bwaldvogel.mongo.bson.Document;

public abstract class TerminalStage implements AggregationStage {

    abstract void applyLast(Stream<Document> stream);

    @Override
    public final Stream<Document> apply(Stream<Document> stream) {
        applyLast(stream);
        return Stream.empty();
    }
}
