package de.bwaldvogel.mongo.oplog;

import java.util.List;

import de.bwaldvogel.mongo.backend.TailableCursor;
import de.bwaldvogel.mongo.backend.aggregation.Aggregation;
import de.bwaldvogel.mongo.bson.Document;

public interface Oplog {

    void handleInsert(String namespace, List<Document> documents);

    void handleUpdate(String namespace, Document selector, Document query, List<Object> modifiedIds);

    void handleDelete(String namespace, Document query, List<Object> deletedIds);

    void handleDropCollection(String namespace);

    TailableCursor createCursor(String namespace, OplogPosition initialOplogPosition);

    TailableCursor createChangeStreamCursor(Document changeStreamDocument, String namespace, Aggregation aggregation);
}
