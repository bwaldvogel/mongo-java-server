package de.bwaldvogel.mongo;

import org.junit.Assume;
import org.junit.jupiter.api.extension.RegisterExtension;

import de.bwaldvogel.mongo.backend.AbstractAggregationTest;

public class RealMongoAggregationTest extends AbstractAggregationTest {

    @RegisterExtension
    static RealMongoContainer realMongoContainer = new RealMongoContainer();

    @Override
    protected void setUpBackend() throws Exception {
        serverAddress = realMongoContainer.getServerAddress();
    }

    @Override
    protected MongoBackend createBackend() throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void testAggregateWithGeoNear() throws Exception {
        Assume.assumeTrue(false);
        super.testAggregateWithGeoNear();
    }
}
