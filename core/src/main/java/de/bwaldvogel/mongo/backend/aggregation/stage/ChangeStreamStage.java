package de.bwaldvogel.mongo.backend.aggregation.stage;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.oplog.Oplog;

import java.util.stream.Stream;

public class ChangeStreamStage implements AggregationStage {

    private final Document changeStreamDocument;
    private final Oplog oplog;

    public ChangeStreamStage(Document changeStreamDocument, Oplog oplog) {
        this.changeStreamDocument = changeStreamDocument;
        this.oplog = oplog;
    }

    @Override
    public Stream<Document> apply(Stream<Document> stream) {
        return oplog.handleAggregation(changeStreamDocument);
    }
}
