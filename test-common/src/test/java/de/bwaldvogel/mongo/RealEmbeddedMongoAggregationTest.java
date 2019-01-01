package de.bwaldvogel.mongo;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

import de.bwaldvogel.mongo.backend.AbstractAggregationTest;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;

public class RealEmbeddedMongoAggregationTest extends AbstractAggregationTest {

    private static final MongodStarter starter = MongodStarter.getDefaultInstance();
    private static MongodExecutable mongodExecutable;
    private static InetSocketAddress embeddedServerAddress;

    @BeforeClass
    public static void setUpServer() throws IOException {
        String bindIp = "localhost";
        int port = 12345;
        IMongodConfig mongodConfig = new MongodConfigBuilder()
            .version(Version.Main.PRODUCTION)
            .net(new Net(bindIp, port, Network.localhostIsIPv6()))
            .build();
        embeddedServerAddress = new InetSocketAddress(bindIp, port);
        mongodExecutable = starter.prepare(mongodConfig);
        mongodExecutable.start();
    }

    @AfterClass
    public static void tearDownServer() {
        mongodExecutable.stop();
    }

    @Override
    protected void setUpBackend() throws Exception {
        serverAddress = embeddedServerAddress;
        try (MongoClient client = new MongoClient(new ServerAddress(serverAddress))) {
            client.dropDatabase(TEST_DATABASE_NAME);
        }
    }

    @Override
    protected void tearDownBackend() {
        // noop
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        db.drop();
    }

    @Override
    protected MongoBackend createBackend() throws Exception {
        throw new UnsupportedOperationException();
    }

}
