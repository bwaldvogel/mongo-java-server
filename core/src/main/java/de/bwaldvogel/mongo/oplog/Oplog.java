package de.bwaldvogel.mongo.oplog;

import java.util.List;

import de.bwaldvogel.mongo.backend.Cursor;
import de.bwaldvogel.mongo.bson.Document;

public interface Oplog {

    void handleInsert(String namespace, List<Document> documents);

    void handleUpdate(String namespace, Document selector, Document query, List<Object> modifiedIds);

    void handleDelete(String namespace, Document query, List<Object> deletedIds);

    Cursor createCursor(Document changeStreamDocument);
}
