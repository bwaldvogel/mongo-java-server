package de.bwaldvogel.mongo.backend.memory;

import de.bwaldvogel.mongo.backend.OplogMongoBackend;

public class MemoryBackend extends OplogMongoBackend {

    @Override
    public MemoryDatabase openOrCreateDatabase(String databaseName) {
        return new MemoryDatabase(this, databaseName);
    }

}
