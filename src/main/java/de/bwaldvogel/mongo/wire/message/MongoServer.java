package de.bwaldvogel.mongo.wire.message;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteOrder;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.HeapChannelBufferFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import de.bwaldvogel.mongo.backend.MongoBackend;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;
import de.bwaldvogel.mongo.wire.MongoDatabaseHandler;
import de.bwaldvogel.mongo.wire.MongoWireEncoder;
import de.bwaldvogel.mongo.wire.MongoWireProtocolHandler;

public class MongoServer {

    private MongoBackend backend;

    private ChannelFactory factory;
    private Channel serverChannel;

    /**
     * creates a mongo server with in-memory backend
     */
    public MongoServer() {
        this( new MemoryBackend() );
    }

    public MongoServer(MongoBackend backend) {
        this.backend = backend;
    }

    public void bind( SocketAddress socketAddress ){
        factory = new NioServerSocketChannelFactory( Executors.newCachedThreadPool() , Executors.newCachedThreadPool() );
        final ServerBootstrap bootstrap = new ServerBootstrap( factory );
        bootstrap.setOption( "child.bufferFactory", new HeapChannelBufferFactory( ByteOrder.LITTLE_ENDIAN ) );

        // Set up the pipeline factory.
        bootstrap.setPipelineFactory( new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() throws Exception{
                return Channels.pipeline( new MongoWireEncoder(), new MongoWireProtocolHandler(), new MongoDatabaseHandler( backend ) );
            }
        } );

        serverChannel = bootstrap.bind( socketAddress );
    }

    /**
     * starts and binds the server on a local random port
     *
     * @return the random local address the server was bound to
     */
    public InetSocketAddress bind(){
        bind( new InetSocketAddress( "localhost" , 0 ) );
        return (InetSocketAddress) serverChannel.getLocalAddress();
    }

    public void shutdown(){
        if ( serverChannel != null ) {
            serverChannel.close().awaitUninterruptibly();
            serverChannel = null;
        }

        if ( factory != null ) {
            factory.releaseExternalResources();
            factory = null;
        }
    }

}
