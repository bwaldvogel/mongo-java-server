package de.bwaldvogel.mongo.backend.memory;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.backend.AbstractOplogTest;
import de.bwaldvogel.mongo.backend.h2.H2Backend;

class H2OplogTest extends AbstractOplogTest {

    @Override
    protected MongoBackend createBackend() {
        return H2Backend.inMemory(clock);
    }

}
