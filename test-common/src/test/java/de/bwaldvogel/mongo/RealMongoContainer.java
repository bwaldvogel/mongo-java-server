package de.bwaldvogel.mongo;

import org.testcontainers.containers.GenericContainer;

public final class RealMongoContainer {

    private RealMongoContainer() {
    }

    public static GenericContainer<?> start() {
        return start("4.0.13");
    }

    public static GenericContainer<?> start(String version) {
        GenericContainer<?> mongoContainer = new GenericContainer<>("mongo:" + version).withExposedPorts(27017);
        mongoContainer.start();
        return mongoContainer;
    }
}
