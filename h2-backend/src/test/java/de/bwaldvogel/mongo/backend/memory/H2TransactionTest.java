package de.bwaldvogel.mongo.backend.memory;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.ServerVersion;
import de.bwaldvogel.mongo.backend.AbstractTransactionTest;
import de.bwaldvogel.mongo.backend.h2.H2Backend;

public class H2TransactionTest extends AbstractTransactionTest {
    @Override
    protected MongoBackend createBackend() throws Exception {
        return H2Backend.inMemory(clock).version(ServerVersion.MONGO_4_2);
    }
}
