package de.bwaldvogel.mongo;

import static org.testcontainers.containers.Network.newNetwork;

import java.net.InetSocketAddress;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

public final class RealMongoReplicaSet {
    public static RealMongoReplicaSet INSTANCE = new RealMongoReplicaSet();
    private static final int MONGO_PORT = 27017;
    private static final String MONGO_IMAGE = "mongo:4.2.8";

    private InetSocketAddress address;

    @SuppressWarnings("checkstyle:IllegalCatch")
    private RealMongoReplicaSet() {
        Network network = newNetwork();

        GenericContainer<?> m1 = new GenericContainer<>(MONGO_IMAGE)
            .withNetwork(network)
            .withNetworkAliases("M1")
            .withExposedPorts(MONGO_PORT)
            .withCommand("--replSet rs0 --bind_ip localhost,M1");

        GenericContainer<?> m2 = new GenericContainer<>(MONGO_IMAGE)
            .withNetwork(network)
            .withNetworkAliases("M2")
            .withExposedPorts(MONGO_PORT)
            .withCommand("--replSet rs0 --bind_ip localhost,M2");

        GenericContainer<?> m3 = new GenericContainer<>(MONGO_IMAGE)
            .withNetwork(network)
            .withNetworkAliases("M3")
            .withExposedPorts(MONGO_PORT)
            .withCommand("--replSet rs0 --bind_ip localhost,M3");

        m1.start();
        m2.start();
        m3.start();

        try {
            m1.execInContainer("/bin/bash", "-c",
                "mongo --eval 'printjson(rs.initiate({_id:\"rs0\","
                    + "members:[{_id:0,host:\"M1:27017\"},{_id:1,host:\"M2:27017\"},{_id:2,host:\"M3:27017\"}]}))' "
                    + "--quiet");
            m1.execInContainer("/bin/bash", "-c",
                "until mongo --eval \"printjson(rs.isMaster())\" | grep ismaster | grep true > /dev/null 2>&1;"
                    + "do sleep 1;done");
        } catch (Exception e) {
            throw new IllegalStateException("Failed to initiate rs.", e);
        }

        address = new InetSocketAddress(m1.getContainerIpAddress(), m1.getFirstMappedPort());
    }

    public InetSocketAddress getAddress() {
        return address;
    }

}