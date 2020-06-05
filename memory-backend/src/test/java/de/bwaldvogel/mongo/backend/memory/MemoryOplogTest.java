package de.bwaldvogel.mongo.backend.memory;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.backend.AbstractOplogTest;

public class MemoryOplogTest extends AbstractOplogTest {

    @Override
    protected MongoBackend createBackend() {
        return new MemoryBackend(clock);
    }

}
