package de.bwaldvogel.mongo.backend.memory;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.backend.AbstractFakeBackendTest;

public class MemoryBackendVersion3_4Test extends AbstractFakeBackendTest {

    @Override
    protected MongoBackend createBackend() {
        MemoryBackend backend = new MemoryBackend();
        backend.setVersion(3, 4, 0);
        return backend;
    }

}