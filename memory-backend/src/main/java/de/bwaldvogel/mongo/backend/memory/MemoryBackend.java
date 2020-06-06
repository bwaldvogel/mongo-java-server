package de.bwaldvogel.mongo.backend.memory;

import java.time.Clock;

import de.bwaldvogel.mongo.backend.AbstractMongoBackend;

public class MemoryBackend extends AbstractMongoBackend {

    public MemoryBackend() {
    }

    public MemoryBackend(Clock clock) {
        super(clock);
    }

    @Override
    public MemoryDatabase openOrCreateDatabase(String databaseName) {
        return new MemoryDatabase(databaseName, getCursorRegistry());
    }

}
