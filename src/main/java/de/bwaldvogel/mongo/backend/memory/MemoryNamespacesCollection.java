package de.bwaldvogel.mongo.backend.memory;

public class MemoryNamespacesCollection extends MemoryCollection {

    public MemoryNamespacesCollection(String databaseName) {
        super(databaseName, "system.namespaces", "name");
    }

}
