package de.bwaldvogel.mongo.backend.memory;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.ServerVersion;
import de.bwaldvogel.mongo.backend.AbstractBackendTest;

public class MemoryBackendVersion3_6Test extends AbstractBackendTest {

    @Override
    protected MongoBackend createBackend() {
        return new MemoryBackend(clock).version(ServerVersion.MONGO_3_6);
    }

}
