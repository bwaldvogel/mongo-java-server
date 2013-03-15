package de.bwaldvogel.mongo.backend.memory;

public class MemoryIndexesCollection extends MemoryCollection {

    public MemoryIndexesCollection(String databaseName) {
        super(databaseName, "system.indexes", "name");
    }

}
