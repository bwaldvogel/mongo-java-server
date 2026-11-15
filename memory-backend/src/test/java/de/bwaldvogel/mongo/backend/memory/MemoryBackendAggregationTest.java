package de.bwaldvogel.mongo.backend.memory;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.backend.AbstractAggregationTest;

class MemoryBackendAggregationTest extends AbstractAggregationTest {

    @Override
    protected MongoBackend createBackend() {
        return new MemoryBackend(clock);
    }

}
