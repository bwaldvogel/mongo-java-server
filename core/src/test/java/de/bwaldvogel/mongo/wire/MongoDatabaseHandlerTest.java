package de.bwaldvogel.mongo.wire;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import io.netty.channel.Channel;
import org.junit.Test;

import static de.bwaldvogel.mongo.TestUtils.json;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class MongoDatabaseHandlerTest {

    @Test
    public void testWrappedCommand() {
        final MongoBackend backend = mock(MongoBackend.class);
        final Channel channel = mock(Channel.class);

        final Document queryDoc = json("'$query': { 'count': 'collectionName' }, '$readPreference': { 'mode': 'secondaryPreferred' }");
        final Document subQueryDoc = json("'count': 'collectionName'");
        final MongoQuery query = new MongoQuery(channel, null, "dbName.$cmd", 0, 0, queryDoc, null);

        final MongoDatabaseHandler handler = new MongoDatabaseHandler(backend, null);

        handler.handleCommand(channel, query);

        verify(backend).handleCommand(channel, "dbName", "count", subQueryDoc);
    }

    @Test
    public void testNonWrappedCommand() {
        final MongoBackend backend = mock(MongoBackend.class);
        final Channel channel = mock(Channel.class);

        final Document queryDoc = json("'count': 'collectionName'");
        final MongoQuery query = new MongoQuery(channel, null, "dbName.$cmd", 0, 0, queryDoc, null);

        final MongoDatabaseHandler handler = new MongoDatabaseHandler(backend, null);

        handler.handleCommand(channel, query);

        verify(backend).handleCommand(channel, "dbName", "count", queryDoc);
    }
}
