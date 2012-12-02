package de.bwaldvogel.mongo.wire;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.bson.BSONObject;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import de.bwaldvogel.mongo.backend.MongoBackend;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.exception.MongoServerException;
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
    public void channelClosed( ChannelHandlerContext ctx , ChannelStateEvent e ) throws Exception {
        int clientId = e.getChannel().getId().intValue();
        log.info( "client " + clientId + " closed" );
        mongoBackend.handleClose( clientId );
        super.channelClosed( ctx, e );
    }

    @Override
    public void messageReceived( ChannelHandlerContext ctx , MessageEvent event ) throws MongoServerException {
        int clientId = event.getChannel().getId().intValue();
        final Object object = event.getMessage();
        if ( object instanceof MongoQuery ) {
            event.getChannel().write( handleQuery( clientId, (MongoQuery) object ) );
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
            throw new MongoServerException( "unknown message: " + object );
        }
    }

    protected MongoReply handleQuery( int clientId , MongoQuery query ) {
        List<BSONObject> documents = new ArrayList<BSONObject>();
        MessageHeader header = new MessageHeader( idSequence.incrementAndGet() , query.getHeader().getRequestID() );
        try {
            if ( query.getCollectionName().equals( "$cmd" ) ) {
                String command = query.getQuery().keySet().iterator().next();
                BSONObject result = mongoBackend.handleCommand( clientId, query.getDatabaseName(), command, query.getQuery() );
                documents.add( result );
            }
            else {
                for ( BSONObject obj : mongoBackend.handleQuery( query ) ) {
                    documents.add( obj );
                }
            }
        }
        catch ( MongoServerError e ) {
            log.error( "failed to handle query " + query, e );
            documents.add( e.createBSONObject( clientId ) );
        }
        catch ( MongoServerException e ) {
            log.error( "failed to handle query " + query, e );
            documents.add( e.createBSONObject( query.getQuery() ) );
        }

        return new MongoReply( header , documents );
    }
}
