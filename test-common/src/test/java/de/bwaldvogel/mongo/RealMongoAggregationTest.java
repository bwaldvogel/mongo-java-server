package de.bwaldvogel.mongo;

import java.net.InetSocketAddress;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;

import de.bwaldvogel.mongo.backend.AbstractAggregationTest;

public class RealMongoAggregationTest extends AbstractAggregationTest {

    private static GenericContainer<?> mongoContainer;

    @BeforeAll
    public static void setUpMongoContainer() {
        mongoContainer = RealMongoContainer.start();
    }

    @Override
    protected void setUpBackend() throws Exception {
        serverAddress = new InetSocketAddress(mongoContainer.getFirstMappedPort());
    }

    @AfterAll
    public static void tearDownServer() {
        mongoContainer.stop();
        mongoContainer = null;
    }

    @Override
    protected MongoBackend createBackend() throws Exception {
        throw new UnsupportedOperationException();
    }

}
