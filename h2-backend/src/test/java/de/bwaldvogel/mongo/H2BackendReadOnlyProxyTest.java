package de.bwaldvogel.mongo;

import de.bwaldvogel.AbstractReadOnlyProxyTest;
import de.bwaldvogel.mongo.backend.h2.H2Backend;

class H2BackendReadOnlyProxyTest extends AbstractReadOnlyProxyTest {

    @Override
    protected MongoBackend createBackend() {
        return H2Backend.inMemory();
    }

}
