package de.bwaldvogel.mongo;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.backend.MongoBackend;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;
import de.bwaldvogel.mongo.wire.MongoDatabaseHandler;
import de.bwaldvogel.mongo.wire.MongoExceptionHandler;
import de.bwaldvogel.mongo.wire.MongoWireEncoder;
import de.bwaldvogel.mongo.wire.MongoWireProtocolHandler;

public class MongoServer {

    private static final Logger log = LoggerFactory.getLogger(MongoServer.class);

    private MongoBackend backend;

    private EventLoopGroup bossGroup;

    private EventLoopGroup workerGroup;

    private ChannelGroup channelGroup;

    private Channel channel;

    public static void main(String[] args) throws Exception {
        final MongoServer mongoServer = new MongoServer();
        mongoServer.bind(new InetSocketAddress(InetAddress.getByAddress(new byte[] { 0, 0, 0, 0 }), 27017));
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.info("shutting down {}", mongoServer);
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

        bossGroup = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup();
        channelGroup = new DefaultChannelGroup("mongodb-channels", workerGroup.next());

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap//
                    .group(bossGroup, workerGroup)//
                    .channel(NioServerSocketChannel.class)//
                    .option(ChannelOption.SO_BACKLOG, 100)//
                    .localAddress(socketAddress)//
                    .childOption(ChannelOption.TCP_NODELAY, true)//
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline().addLast(new MongoWireEncoder());
                            ch.pipeline().addLast(new MongoWireProtocolHandler());
                            ch.pipeline().addLast(new MongoDatabaseHandler(backend, channelGroup));
                            ch.pipeline().addLast(new MongoExceptionHandler());
                        }
                    });

            channel = bootstrap.bind().syncUninterruptibly().channel();

            log.info("started {}", this);
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

    /**
     * @return the local address the server was bound or null if the server is
     *         not listening
     */
    public InetSocketAddress getLocalAddress() {
        if (channel == null)
            return null;
        return (InetSocketAddress) channel.localAddress();
    }

    /**
     * Stop accepting new clients. Wait until all resources (such as client
     * connection) are closed and then shutdown. This method blocks until all
     * clients are finished. Use {@link #shutdownNow()} if the shutdown should
     * be forced.
     */
    public void shutdown() {
        stopListenting();

        // Shut down all event loops to terminate all threads.
        bossGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
        workerGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);

        bossGroup.terminationFuture().syncUninterruptibly();
        workerGroup.terminationFuture().syncUninterruptibly();

        log.info("completed shutdown of {}", this);
    }

    /**
     * Closes the server socket. No new clients are accepted afterwards.
     */
    public void stopListenting() {
        if (channel != null) {
            log.info("closing server channel");
            channel.close().syncUninterruptibly();
            channel = null;
        }
    }

    /**
     * Stops accepting new clients, closes all clients and finally shuts down
     * the server In contrast to {@link #shutdown()}, this method should not
     * block.
     */
    public void shutdownNow() {
        stopListenting();
        closeClients();
        shutdown();
    }

    private void closeClients() {
        final int numClients = channelGroup.size();
        if (numClients > 0) {
            log.warn("Closing {} clients", numClients);
        }
        channelGroup.close().syncUninterruptibly();
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
