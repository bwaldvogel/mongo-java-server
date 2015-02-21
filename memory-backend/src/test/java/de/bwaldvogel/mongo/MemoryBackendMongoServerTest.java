package de.bwaldvogel.mongo;

import de.bwaldvogel.mongo.backend.memory.MemoryBackend;

public class MemoryBackendMongoServerTest extends MongoServerTest {

    @Override
    protected MongoBackend createBackend() {
        return new MemoryBackend();
    }

}
