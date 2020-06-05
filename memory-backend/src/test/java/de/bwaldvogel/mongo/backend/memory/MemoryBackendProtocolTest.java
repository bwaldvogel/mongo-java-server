package de.bwaldvogel.mongo.backend.memory;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.backend.AbstractProtocolTest;

public class MemoryBackendProtocolTest extends AbstractProtocolTest {

    @Override
    protected MongoBackend createBackend() throws Exception {
        return new MemoryBackend(clock);
    }

}
