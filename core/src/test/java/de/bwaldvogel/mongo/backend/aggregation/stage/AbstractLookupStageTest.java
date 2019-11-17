package de.bwaldvogel.mongo.backend.aggregation.stage;

import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.MongoDatabase;

@RunWith(MockitoJUnitRunner.class)
public abstract class AbstractLookupStageTest {

    @Mock
    MongoDatabase database;

    @Mock
    @SuppressWarnings("rawtypes")
    MongoCollection authorsCollection;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() {
        when(database.resolveCollection("authors", false)).thenReturn(authorsCollection);
    }

}
