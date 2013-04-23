package de.bwaldvogel.mongo;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteOrder;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.HeapChannelBufferFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import de.bwaldvogel.mongo.backend.MongoBackend;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;
import de.bwaldvogel.mongo.wire.MongoDatabaseHandler;
import de.bwaldvogel.mongo.wire.MongoWireEncoder;
import de.bwaldvogel.mongo.wire.MongoWireProtocolHandler;

public class MongoServer {

    private static final Logger log = Logger.getLogger(MongoServer.class);

    public static final String VERSION = "0.1";

    private MongoBackend backend;

    private ChannelFactory factory;
    private ChannelGroup channelGroup = new DefaultChannelGroup(getClass().getSimpleName());
    private Channel serverChannel;

    public static void main(String[] args) throws Exception {
        final MongoServer mongoServer = new MongoServer();
        mongoServer.bind(new InetSocketAddress(InetAddress.getByAddress(new byte[] { 0, 0, 0, 0 }), 27017));
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.info("shutting down " + mongoServer);
                mongoServer.shutdownNow();
            }
        });
    }

    /**
     * creates a mongo server with in-memory backend
     */
    public MongoServer() {
        this(new MemoryBackend());
    }

    public MongoServer(MongoBackend backend) {
        this.backend = backend;
    }

    public void bind(SocketAddress socketAddress) {
        factory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
        final ServerBootstrap bootstrap = new ServerBootstrap(factory);
        bootstrap.setOption("child.bufferFactory", new HeapChannelBufferFactory(ByteOrder.LITTLE_ENDIAN));

        // Set up the pipeline factory.
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() throws Exception {
                return Channels.pipeline(new MongoWireEncoder(), new MongoWireProtocolHandler(),
                        new MongoDatabaseHandler(backend, channelGroup));
            }
        });

        try {
            serverChannel = bootstrap.bind(socketAddress);
            log.info("started " + this);
        } catch (RuntimeException e) {
            shutdownNow();
            throw e;
        }
    }

    /**
     * starts and binds the server on a local random port
     *
     * @return the random local address the server was bound to
     */
    public InetSocketAddress bind() {
        bind(new InetSocketAddress("localhost", 0));
        return getLocalAddress();
    }

    protected InetSocketAddress getLocalAddress() {
        if (serverChannel == null)
            return null;
        return (InetSocketAddress) serverChannel.getLocalAddress();
    }

    /**
     * Stop accepting new clients. Wait until all resources (such as client connection) are closed and then shutdown.
     * This method blocks until all clients are finished.
     * Use {@link #shutdownNow()} if the shutdown should be forced.
     */
    public void shutdown() {
        stopListenting();

        if (factory != null) {
            factory.releaseExternalResources();
            factory = null;
        }

        log.info("completed shutdown of " + this);
    }

    /**
     * Closes the server socket. No new clients are accepted afterwards.
     */
    public void stopListenting() {
        if (serverChannel != null) {
            log.info("closing server channel");
            serverChannel.close().awaitUninterruptibly();
            serverChannel = null;
        }
    }

    /**
     * Stops accepting new clients, closes all clients and finally shuts down the server
     * In contrast to {@link #shutdown()}, this method should not block.
     */
    public void shutdownNow() {
        // stop accepting new clients, before all remaining clients can be killed
        stopListenting();
        closeClients();
        // ready to finally shutdown and close all remaining resources
        // should not block
        shutdown();
    }

    private void closeClients() {
        if (!channelGroup.isEmpty()) {
            log.warn(channelGroup.size() + " channels still open. closing now...");
            channelGroup.close().awaitUninterruptibly();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(getClass().getSimpleName());
        sb.append("(");
        InetSocketAddress socketAddress = getLocalAddress();
        if (socketAddress != null) {
            sb.append("port: ").append(socketAddress.getPort());
        }
        sb.append(")");
        return sb.toString();
    }
}
