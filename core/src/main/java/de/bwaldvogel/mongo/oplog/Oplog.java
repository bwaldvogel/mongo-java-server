package de.bwaldvogel.mongo.oplog;

import java.util.List;
import java.util.stream.Stream;

import de.bwaldvogel.mongo.backend.Cursor;
import de.bwaldvogel.mongo.backend.aggregation.Aggregation;
import de.bwaldvogel.mongo.bson.Document;

public interface Oplog {

    void handleInsert(String namespace, List<Document> documents);

    void handleUpdate(String namespace, Document selector, Document query, List<Object> modifiedIds);

    void handleDelete(String namespace, Document query, List<Object> deletedIds);

    default Stream<Document> handleAggregation(Document changeStreamDocument) {
        return Stream.empty();
    };

    Cursor createCursor(Aggregation aggregation);

    Cursor createCursor(Aggregation aggregation, long resumeToken);
}
