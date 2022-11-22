package de.bwaldvogel.mongo.backend;

import de.bwaldvogel.mongo.MongoDatabase;

@FunctionalInterface
public interface DatabaseResolver {

    MongoDatabase resolve(String databaseName);

}
