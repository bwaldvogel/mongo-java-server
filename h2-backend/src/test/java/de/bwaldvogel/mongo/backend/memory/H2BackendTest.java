package de.bwaldvogel.mongo.backend.memory;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.backend.AbstractBackendTest;
import de.bwaldvogel.mongo.backend.h2.H2Backend;

public class H2BackendTest extends AbstractBackendTest {

    @Override
    protected MongoBackend createBackend() throws Exception {
        return H2Backend.inMemory();
    }

}
