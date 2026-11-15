package de.bwaldvogel.mongo;

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
    protected void setUpBackend() {
        connectionString = RealMongoContainer.getConnectionString(mongoContainer);
    }

    @AfterAll
    public static void tearDownServer() {
        mongoContainer.stop();
        mongoContainer = null;
    }

    @Override
    protected void restart() {
        tearDown();
        tearDownServer();
        setUp();
    }

    @Override
    protected MongoBackend createBackend() {
        throw new UnsupportedOperationException();
    }

}
