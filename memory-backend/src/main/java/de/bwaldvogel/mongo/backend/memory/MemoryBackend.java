package de.bwaldvogel.mongo.backend.memory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.backend.AbstractMongoBackend;
import de.bwaldvogel.mongo.exception.MongoServerException;

public class MemoryBackend extends AbstractMongoBackend {

    public static final Logger log = LoggerFactory.getLogger(MemoryBackend.class);

    @Override
    public MemoryDatabase openOrCreateDatabase(String databaseName) throws MongoServerException {
        return new MemoryDatabase(this, databaseName);
    }

    @Override
    public void close() {
        // no-op
    }

}
