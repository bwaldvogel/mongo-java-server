package de.bwaldvogel.mongo;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import de.bwaldvogel.mongo.backend.AbstractAggregationTest;

public class RealMongoAggregationTest extends AbstractAggregationTest {

    @RegisterExtension
    static RealMongoContainer realMongoContainer = new RealMongoContainer();

    @Override
    protected void setUpBackend() {
        connectionString = realMongoContainer.getConnectionString();
    }

    @Override
    protected MongoBackend createBackend() {
        throw new UnsupportedOperationException();
    }

    @Test
    @Override
    @Disabled
    public void testAggregateWithGeoNear() {
        super.testAggregateWithGeoNear();
    }
}
