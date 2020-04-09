package de.bwaldvogel.mongo;

import org.testcontainers.containers.GenericContainer;

public final class RealMongoContainer {

    private RealMongoContainer() {
    }

    public static GenericContainer<?> start() {
        GenericContainer<?> mongoContainer = new GenericContainer<>("mongo:4.2.5").withExposedPorts(27017);
        mongoContainer.start();
        return mongoContainer;
    }

}
