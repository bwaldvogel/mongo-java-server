package de.bwaldvogel;

import java.net.InetSocketAddress;
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
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;

import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.wire.MongoDatabaseHandler;
import de.bwaldvogel.mongo.wire.MongoWireEncoder;
import de.bwaldvogel.mongo.wire.MongoWireProtocolHandler;

public class SimpleTest {
    private static final Logger _log = Logger.getLogger( SimpleTest.class );

    public static void main( String[] args ) throws Exception{
        _log.info( "starting" );

        final MongoServer mongoServer = new MongoServer();

        // Configure the server.
        final ChannelFactory factory = new NioServerSocketChannelFactory( Executors.newCachedThreadPool() , Executors.newCachedThreadPool() );
        final ServerBootstrap bootstrap = new ServerBootstrap( factory );
        bootstrap.setOption( "child.bufferFactory", new HeapChannelBufferFactory( ByteOrder.LITTLE_ENDIAN ) );

        // Set up the pipeline factory.
        bootstrap.setPipelineFactory( new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() throws Exception{
                return Channels.pipeline( new MongoWireEncoder(), new MongoWireProtocolHandler(), new MongoDatabaseHandler( mongoServer ) );
            }
        } );

        // Bind and start to accept incoming connections.
        final Channel serverChannel = bootstrap.bind( new InetSocketAddress( 27017 ) );

        _log.info( "bound" );
        try {
            final Mongo mongo = new Mongo( "localhost" );
            final DBCollection collection = mongo.getDB( "testdb" ).getCollection( "testcollection" );
            collection.findOne( new BasicDBObject( "_id" , 1 ) );
            // _log.info( "collection size: " + collection.count() );
            mongo.close();
            _log.info( "mongo closed" );
        }
        catch ( final Exception e ) {
            _log.error( e.getMessage() );
        }
        finally {
            serverChannel.close().awaitUninterruptibly();
            factory.releaseExternalResources();
        }
    }
}
