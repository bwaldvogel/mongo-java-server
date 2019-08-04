package de.bwaldvogel.mongo;

import org.junit.AfterClass;

import de.bwaldvogel.mongo.backend.AbstractPerformanceTest;

public class RealEmbeddedMongoPerformanceTest extends AbstractPerformanceTest {

    private static RealEmbeddedMongo realEmbeddedMongo = new RealEmbeddedMongo();

    @Override
    protected void setUpBackend() throws Exception {
        serverAddress = realEmbeddedMongo.setUp();
    }

    @AfterClass
    public static void tearDownServer() {
        realEmbeddedMongo.stop();
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
