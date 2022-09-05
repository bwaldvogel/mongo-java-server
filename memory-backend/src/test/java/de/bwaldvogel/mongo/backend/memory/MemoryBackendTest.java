package de.bwaldvogel.mongo.backend.memory;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.backend.AbstractBackendTest;

class MemoryBackendTest extends AbstractBackendTest {

    @Override
    protected MongoBackend createBackend() {
        return new MemoryBackend(clock);
    }

}
