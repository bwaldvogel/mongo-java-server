package de.bwaldvogel.mongo.backend.memory;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.backend.AbstractBackendTest;

public class MemoryBackendVersion3_6Test extends AbstractBackendTest {

    @Override
    protected MongoBackend createBackend() {
        MemoryBackend backend = new MemoryBackend(clock);
        backend.setVersion(3, 6, 0);
        backend.setWireVersion(6, 0);
        return backend;
    }

}
