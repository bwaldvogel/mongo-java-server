package de.bwaldvogel.mongo.backend;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mockito;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.bson.Document;

class AbstractMongoDatabaseTest {

    private AbstractMongoDatabase<Object> database;
    private CursorRegistry cursorRegistry;

    @BeforeEach
    public void setup() {
        cursorRegistry = Mockito.mock(CursorRegistry.class);

        database = new AbstractMongoDatabase<Object>("testdb", cursorRegistry) {

            @Override
            protected long getFileSize() {
                return 0;
            }

            @Override
            protected long getStorageSize() {
                return 0;
            }

            @Override
            protected Index<Object> openOrCreateUniqueIndex(String collectionName, String indexName, List<IndexKey> keys, boolean sparse) {
                return null;
            }

            @Override
            protected MongoCollection<Object> openOrCreateCollection(String collectionName, CollectionOptions options) {
                MongoCollection<Object> collection = (MongoCollection<Object>) Mockito.mock(MongoCollection.class);
                when(collection.getCollectionName()).thenReturn("mockCollection");

                QueryResult queryResult = new QueryResult(List.of(new Document("_id", 1)));

                when(collection.handleQuery(any(QueryParameters.class))).thenReturn(queryResult);

                return collection;
            }
        };

        database.initializeNamespacesAndIndexes();
    }

}
