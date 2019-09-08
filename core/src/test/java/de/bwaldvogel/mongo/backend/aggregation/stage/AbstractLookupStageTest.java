package de.bwaldvogel.mongo.backend.aggregation.stage;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.MongoDatabase;

public class AbstractLookupStageTest {

    MongoDatabase database;

    @SuppressWarnings("rawtypes")
    MongoCollection authorsCollection;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() {
        database = mock(MongoDatabase.class);
        authorsCollection = mock(MongoCollection.class);
        when(database.resolveCollection("authors", false)).thenReturn(authorsCollection);
    }

}
