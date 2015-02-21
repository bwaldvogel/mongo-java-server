package de.bwaldvogel.mongo.backend.h2;

import java.util.HashSet;
import java.util.Set;

import org.h2.mvstore.MVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.AbstractMongoBackend;
import de.bwaldvogel.mongo.exception.MongoServerException;


public class H2Backend extends AbstractMongoBackend {

    private static final Logger log = LoggerFactory.getLogger(H2Backend.class);

    private MVStore mvStore;

    public static H2Backend inMemory() {
        MVStore mvStore = MVStore.open(null);
        return new H2Backend(mvStore);
    }

    public H2Backend(MVStore mvStore) {
        this.mvStore = mvStore;

        Set<String> databases = new HashSet<String>();
        for (String mapName : mvStore.getMapNames()) {
            String databaseName = mapName.substring(0, mapName.indexOf('.'));
            databases.add(databaseName);
        }

        for (String database : databases) {
            log.info("opening database '{}'", database);
            try {
                resolveDatabase(database);
            } catch (MongoServerException e) {
                log.error("Failed to open {}", e);
            }
        }
    }

    public H2Backend(String fileName) {
        this(MVStore.open(fileName));
    }

    @Override
    protected MongoDatabase openOrCreateDatabase(String databaseName) throws MongoServerException {
        return new H2Database(databaseName, this, mvStore);
    }

    @Override
    public void close() {
        log.info("closing {}", this);
        mvStore.close();
    }

}
