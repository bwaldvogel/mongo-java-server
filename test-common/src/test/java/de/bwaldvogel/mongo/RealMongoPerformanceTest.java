package de.bwaldvogel.mongo;

import java.net.InetSocketAddress;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.testcontainers.containers.GenericContainer;

import de.bwaldvogel.mongo.backend.AbstractPerformanceTest;

public class RealMongoPerformanceTest extends AbstractPerformanceTest {

    private static GenericContainer<?> mongoContainer;

    @BeforeClass
    public static void setUpMongoContainer() {
        mongoContainer = RealMongoContainer.start("4.2.3");
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
