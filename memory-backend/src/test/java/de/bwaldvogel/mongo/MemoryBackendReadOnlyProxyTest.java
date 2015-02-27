package de.bwaldvogel.mongo;

import de.bwaldvogel.AbstractReadOnlyProxyTest;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;

public class MemoryBackendReadOnlyProxyTest extends AbstractReadOnlyProxyTest {

    @Override
    protected MongoBackend createBackend() {
        return new MemoryBackend();
    }

}
