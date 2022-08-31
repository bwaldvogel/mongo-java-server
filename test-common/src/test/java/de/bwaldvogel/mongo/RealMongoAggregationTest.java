package de.bwaldvogel.mongo;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.extension.RegisterExtension;

import de.bwaldvogel.mongo.backend.AbstractAggregationTest;

public class RealMongoAggregationTest extends AbstractAggregationTest {

    @RegisterExtension
    static RealMongoContainer realMongoContainer = new RealMongoContainer();

    @Override
    protected void setUpBackend() throws Exception {
        connectionString = realMongoContainer.getConnectionString();
    }

    @Override
    protected MongoBackend createBackend() throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void testAggregateWithGeoNear() throws Exception {
        assumeTrue(false);
        super.testAggregateWithGeoNear();
    }
}
