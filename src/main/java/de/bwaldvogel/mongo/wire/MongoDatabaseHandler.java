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

import de.bwaldvogel.mongo.backend.MongoBackend;
import de.bwaldvogel.mongo.exception.MongoServerException;
import de.bwaldvogel.mongo.exception.NoSuchCommandException;
import de.bwaldvogel.mongo.wire.message.MessageHeader;
import de.bwaldvogel.mongo.wire.message.MongoDelete;
import de.bwaldvogel.mongo.wire.message.MongoInsert;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import de.bwaldvogel.mongo.wire.message.MongoReply;
import de.bwaldvogel.mongo.wire.message.MongoUpdate;

public class MongoDatabaseHandler extends SimpleChannelUpstreamHandler {

    private final AtomicInteger idSequence = new AtomicInteger();
    private final MongoBackend mongoBackend;

    private static final Logger log = Logger.getLogger( MongoWireProtocolHandler.class );

    public MongoDatabaseHandler(MongoBackend mongoBackend) {
        this.mongoBackend = mongoBackend;
    }

    @Override
    public void channelClosed( ChannelHandlerContext ctx , ChannelStateEvent e ) throws Exception{
        int clientId = e.getChannel().getId().intValue();
        log.info( "client " + clientId + " closed" );
        mongoBackend.handleClose( clientId );
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
                for ( BSONObject obj : mongoBackend.handleQuery( query ) ) {
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
            mongoBackend.handleInsert( insert );
        }
        else if ( object instanceof MongoDelete ) {
            MongoDelete delete = (MongoDelete) object;
            mongoBackend.handleDelete( delete );
        }
        else if ( object instanceof MongoUpdate ) {
            MongoUpdate update = (MongoUpdate) object;
            mongoBackend.handleUpdate( update );
        }
        else {
            throw new UnsupportedOperationException( object.toString() );
        }
    }
}
