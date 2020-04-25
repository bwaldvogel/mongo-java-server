package de.bwaldvogel.mongo.oplog;

import de.bwaldvogel.mongo.bson.Document;

public interface Oplog {

    default void handleCommand(String databaseName, String command, Document query) {
        switch (command) {
            case "insert":
                handleInsert(databaseName, query);
                break;
            case "update":
                handleUpdate(databaseName, query);
                break;
            case "delete":
                handleDelete(databaseName, query);
                break;
        }
    }

    void handleInsert(String databaseName, Document query);

    void handleUpdate(String databaseName, Document query);

    void handleDelete(String databaseName, Document query);
}
