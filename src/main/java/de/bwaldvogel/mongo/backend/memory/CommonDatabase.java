package de.bwaldvogel.mongo.backend.memory;

abstract class CommonDatabase implements MongoDatabase {

    private final String databaseName;

    public CommonDatabase(String databaseName) {
        this.databaseName = databaseName;
    }

    @Override
    public final String getDatabaseName() {
        return databaseName;
    }

}
