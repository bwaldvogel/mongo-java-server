package de.bwaldvogel.mongo.backend.memory;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.ServerVersion;
import de.bwaldvogel.mongo.backend.AbstractTransactionTest;

public class MemoryTransactionTest extends AbstractTransactionTest {
    @Override
    protected MongoBackend createBackend() {
        return new MemoryBackend().version(ServerVersion.MONGO_4_2);
    }
}