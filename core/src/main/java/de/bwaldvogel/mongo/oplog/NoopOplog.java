package de.bwaldvogel.mongo.oplog;

import de.bwaldvogel.mongo.bson.Document;

public final class NoopOplog implements Oplog {

    private static final NoopOplog INSTANCE = new NoopOplog();

    public static NoopOplog get() {
        return INSTANCE;
    }

    private NoopOplog() {
    }

    @Override
    public void handleInsert(String databaseName, Document query) {
    }

    @Override
    public void handleUpdate(String databaseName, Document query) {
    }

    @Override
    public void handleDelete(String databaseName, Document query) {
    }
}
