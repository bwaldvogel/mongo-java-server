package de.bwaldvogel.mongo.backend.memory;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.backend.AbstractFakeBackendTest;

public class MemoryBackendTest extends AbstractFakeBackendTest {

    @Override
    protected MongoBackend createBackend() {
        return new MemoryBackend();
    }

}