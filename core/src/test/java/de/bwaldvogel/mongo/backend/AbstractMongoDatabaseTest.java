package de.bwaldvogel.mongo.backend;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;

import de.bwaldvogel.mongo.MongoDatabase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.bson.Document;
import io.netty.channel.Channel;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

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

                QueryResult queryResult = new QueryResult(Collections.singletonList(new Document("_id", 1)));

                when(collection.handleQuery(any(QueryParameters.class))).thenReturn(queryResult);

                when(collection.handleQueryAsync(any())).thenCallRealMethod();

                return collection;
            }
        };

        database.initializeNamespacesAndIndexes();
    }

    @Test
    void testHandleCommandAsyncFindReturnEmpty() throws Exception {
        Channel channel = Mockito.mock(Channel.class);

        Document query = new Document();
        query.put("find", "testCollection");

        CompletionStage<Document> responseFuture = database.handleCommandAsync(channel, "find", query, null, null);
        Document response = responseFuture.toCompletableFuture().get();

        assertThat(response).isNotNull();
        assertThat(response.get("ok")).isEqualTo(1.0);
    }

    @Test
    void testHandleCommandAsyncFindReturnSomething() throws Exception {
        Channel channel = Mockito.mock(Channel.class);

        Document query = new Document();
        query.put("find", "mockCollection");

        CompletionStage<Document> responseFuture = database.handleCommandAsync(channel, "find", query, null, null);
        Document response = responseFuture.toCompletableFuture().get();

        assertThat(response).isNotNull();
        assertThat(response.get("ok")).isEqualTo(1.0);

        Document cursor = (Document) response.get("cursor");
        assertThat(cursor).isNotNull();

        List<Document> firstBatch = (List<Document>) cursor.get("firstBatch");
        assertThat(firstBatch).isNotNull();
        assertThat(firstBatch).hasSize(1);

        Document doc = firstBatch.get(0);
        assertThat(doc).isNotNull();
        assertThat(doc.get("_id")).isEqualTo(1);
    }
}
