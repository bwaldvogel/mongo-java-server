package de.bwaldvogel.mongo.backend.memory;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.backend.AbstractFakeBackendTest;
import de.bwaldvogel.mongo.backend.h2.H2Backend;

public class H2BackendTest extends AbstractFakeBackendTest {

    @Override
    protected MongoBackend createBackend() throws Exception {
        return H2Backend.inMemory();
    }

}