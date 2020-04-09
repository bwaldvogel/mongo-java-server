package de.bwaldvogel.mongo.backend.memory;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.backend.AbstractBackendTest;

public class MemoryBackendVersion3_4Test extends AbstractBackendTest {

    @Override
    protected MongoBackend createBackend() {
        MemoryBackend backend = new MemoryBackend();
        backend.setVersion(3, 4, 0);
        return backend;
    }

}
