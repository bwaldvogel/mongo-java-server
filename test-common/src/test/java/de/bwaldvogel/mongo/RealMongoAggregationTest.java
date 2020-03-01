package de.bwaldvogel.mongo;

import java.net.InetSocketAddress;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.testcontainers.containers.GenericContainer;

import de.bwaldvogel.mongo.backend.AbstractAggregationTest;

public class RealMongoAggregationTest extends AbstractAggregationTest {

    private static GenericContainer<?> mongoContainer;

    @BeforeClass
    public static void setUpMongoContainer() {
        mongoContainer = RealMongoContainer.start();
    }

    @Override
    protected void setUpBackend() throws Exception {
        serverAddress = new InetSocketAddress(mongoContainer.getFirstMappedPort());
    }

    @AfterClass
    public static void tearDownServer() {
        mongoContainer.stop();
        mongoContainer = null;
    }

    @Override
    protected MongoBackend createBackend() throws Exception {
        throw new UnsupportedOperationException();
    }

}
