package de.bwaldvogel.mongo.wire;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import de.bwaldvogel.mongo.exception.MongoServerException;
import de.bwaldvogel.mongo.exception.NoSuchCommandException;
import de.bwaldvogel.mongo.wire.message.MessageHeader;
import de.bwaldvogel.mongo.wire.message.MongoDelete;
import de.bwaldvogel.mongo.wire.message.MongoInsert;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import de.bwaldvogel.mongo.wire.message.MongoReply;
import de.bwaldvogel.mongo.wire.message.MongoServer;
import de.bwaldvogel.mongo.wire.message.MongoUpdate;

public class MongoDatabaseHandler extends SimpleChannelUpstreamHandler {

    private final AtomicInteger idSequence = new AtomicInteger();
    private final MongoServer mongoServer;

    private static final Logger log = Logger.getLogger( MongoWireProtocolHandler.class );

    public MongoDatabaseHandler(MongoServer mongoServer) {
        this.mongoServer = mongoServer;
    }

    @Override
    public void channelClosed( ChannelHandlerContext ctx , ChannelStateEvent e ) throws Exception{
        int clientId = e.getChannel().getId().intValue();
        log.info( "client " + clientId + " closed" );
        mongoServer.handleClose( clientId );
        super.channelClosed( ctx, e );
    }

    @Override
    public void messageReceived( ChannelHandlerContext ctx , MessageEvent event ){
        int clientId = event.getChannel().getId().intValue();
        final Object object = event.getMessage();
        if ( object instanceof MongoQuery ) {
            MongoQuery query = (MongoQuery) object;
            List<BSONObject> documents = new ArrayList<BSONObject>();
            MessageHeader header = new MessageHeader( idSequence.incrementAndGet() , query.getHeader().getRequestID() );
            try {
                for ( BSONObject obj : mongoServer.handleQuery( query ) ) {
                    documents.add( obj );
                }
            }
            catch ( MongoServerException e ) {
                log.error( "failed to handle query " + query, e );
                documents.add( e.createBSONObject( clientId ) );
            }
            catch ( NoSuchCommandException e ) {
                log.error( "illegal command " + query, e );
                documents.add( e.createBSONObject( query.getQuery() ) );
            }
            catch ( RuntimeException e ) {
                log.error( "failed to handle query " + query, e );
                BSONObject obj = new BasicBSONObject();
                obj.put( "err", e.toString() );
                obj.put( "ok", Integer.valueOf( 0 ) );
                documents.add( obj );
            }

            event.getChannel().write( new MongoReply( header , documents ) );
        }
        else if ( object instanceof MongoInsert ) {
            MongoInsert insert = (MongoInsert) object;
            mongoServer.handleInsert( insert );
        }
        else if ( object instanceof MongoDelete ) {
            MongoDelete delete = (MongoDelete) object;
            mongoServer.handleDelete( delete );
        }
        else if ( object instanceof MongoUpdate ) {
            MongoUpdate update = (MongoUpdate) object;
            mongoServer.handleUpdate( update );
        }
        else {
            throw new UnsupportedOperationException( object.toString() );
        }
    }
}
