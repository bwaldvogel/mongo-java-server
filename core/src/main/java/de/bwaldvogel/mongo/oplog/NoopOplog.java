package de.bwaldvogel.mongo.oplog;

import de.bwaldvogel.mongo.backend.Cursor;
import de.bwaldvogel.mongo.backend.EmptyCursor;
import de.bwaldvogel.mongo.backend.aggregation.Aggregation;
import de.bwaldvogel.mongo.bson.Document;

import java.util.List;

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
    public Cursor createCursor(Aggregation aggregation) {
        return EmptyCursor.get();
    }

    @Override
    public Cursor createCursor(Aggregation aggregation, long resumeToken) {
        return EmptyCursor.get();
    }
}
