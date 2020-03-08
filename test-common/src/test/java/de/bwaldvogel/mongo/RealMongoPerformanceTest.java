package de.bwaldvogel.mongo;

import java.net.InetSocketAddress;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;

import de.bwaldvogel.mongo.backend.AbstractPerformanceTest;

public class RealMongoPerformanceTest extends AbstractPerformanceTest {

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
    protected void restart() throws Exception {
        tearDown();
        tearDownServer();
        setUp();
    }

    @Override
    protected MongoBackend createBackend() throws Exception {
        throw new UnsupportedOperationException();
    }

}
