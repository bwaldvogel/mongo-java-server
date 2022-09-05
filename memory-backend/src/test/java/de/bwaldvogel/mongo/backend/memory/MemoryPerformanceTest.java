package de.bwaldvogel.mongo.backend.memory;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.backend.AbstractPerformanceTest;

class MemoryPerformanceTest extends AbstractPerformanceTest {

    @Override
    protected MongoBackend createBackend() {
        return new MemoryBackend();
    }

}
