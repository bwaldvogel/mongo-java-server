package de.bwaldvogel.mongo;

import java.net.InetSocketAddress;

import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;

class RealEmbeddedMongo {

    private static final MongodStarter starter = MongodStarter.getDefaultInstance();
    private MongodExecutable mongodExecutable;

    InetSocketAddress setUp() throws Exception {
        String bindIp = "localhost";
        int port = 12345;
        IMongodConfig mongodConfig = new MongodConfigBuilder()
            .version(Version.Main.PRODUCTION)
            .net(new Net(bindIp, port, Network.localhostIsIPv6()))
            .build();
        InetSocketAddress serverAddress = new InetSocketAddress(bindIp, port);
        mongodExecutable = starter.prepare(mongodConfig);
        mongodExecutable.start();
        return serverAddress;
    }

    void stop() {
        mongodExecutable.stop();
    }

}
