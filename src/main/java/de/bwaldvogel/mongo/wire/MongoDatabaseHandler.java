package de.bwaldvogel.mongo.wire;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import de.bwaldvogel.mongo.MongoQuery;
import de.bwaldvogel.mongo.MongoReply;
import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.collection.MongoCollection;

public class MongoDatabaseHandler extends SimpleChannelUpstreamHandler {

    private final MongoServer mongoServer;

    public MongoDatabaseHandler(MongoServer mongoServer) {
        this.mongoServer = mongoServer;
    }

    @Override
    public void messageReceived( ChannelHandlerContext ctx , MessageEvent e ) throws Exception{
        final Object object = e.getMessage();
        if ( object instanceof MongoQuery ) {
            final MongoQuery query = (MongoQuery) object;
            final MongoCollection collection = mongoServer.getCollection( query.getFullCollectionName() );
            final MongoReply reply = collection.handleQuery( query );
            e.getChannel().write( reply );
        }
        else
            throw new UnsupportedOperationException();
    }

}
