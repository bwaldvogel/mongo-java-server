package de.bwaldvogel.mongo;

import de.bwaldvogel.mongo.backend.h2.H2Backend;

class H2BackendMongoServerTest extends MongoServerTest {

    @Override
    protected MongoBackend createBackend() {
        return H2Backend.inMemory();
    }

}
