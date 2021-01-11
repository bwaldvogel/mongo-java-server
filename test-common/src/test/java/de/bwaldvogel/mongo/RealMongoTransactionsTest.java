package de.bwaldvogel.mongo;

import java.net.InetSocketAddress;

import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.backend.AbstractTransactionTest;

public class RealMongoTransactionsTest extends AbstractTransactionTest {

    private static final Logger log = LoggerFactory.getLogger(RealMongoTransactionsTest.class);
    private static RealMongoReplicaSet mongoReplicaSet;

    @BeforeAll
    public static void setUpMongoContainer() {
        if (Boolean.getBoolean("mongo-java-server-use-existing-container")) {
            log.info("Not starting a test container in favor of an existing container.");
            return;
        }
        mongoReplicaSet = RealMongoReplicaSet.INSTANCE;
    }

    @Override
    protected MongoBackend createBackend() throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void setUpBackend() throws Exception {
        if (mongoReplicaSet != null) {
            serverAddress = mongoReplicaSet.getAddress();
        } else {
            serverAddress = new InetSocketAddress("127.0.0.1", 27018);
        }
    }
}