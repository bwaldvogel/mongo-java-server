package de.bwaldvogel.mongo.oplog;

import java.util.List;

import de.bwaldvogel.mongo.backend.Cursor;
import de.bwaldvogel.mongo.backend.EmptyCursor;
import de.bwaldvogel.mongo.bson.Document;

public final class NoopOplog implements Oplog {

    private static final NoopOplog INSTANCE = new NoopOplog();

    public static NoopOplog get() {
        return INSTANCE;
    }

    private NoopOplog() {
    }

    @Override
    public void handleInsert(String namespace, List<Document> documents) {
    }

    @Override
    public void handleUpdate(String namespace, Document selector, Document query, List<Object> ids) {
    }

    @Override
    public void handleDelete(String namespace, Document query, List<Object> deletedIds) {
    }

    @Override
    public Cursor createCursor(Document changeStreamDocument) {
        return EmptyCursor.get();
    }
}
