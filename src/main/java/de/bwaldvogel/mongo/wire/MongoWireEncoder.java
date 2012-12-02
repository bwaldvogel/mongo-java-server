package de.bwaldvogel.mongo.wire;

import java.nio.ByteOrder;
import java.util.List;

import org.apache.log4j.Logger;
import org.bson.BSON;
import org.bson.BSONObject;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

import de.bwaldvogel.mongo.wire.message.MongoReply;

public class MongoWireEncoder extends OneToOneEncoder {
    private static final Logger _log = Logger.getLogger( MongoWireEncoder.class );

    @Override
    protected Object encode( ChannelHandlerContext ctx , Channel channel , Object msg ) throws Exception {

        final ChannelBuffer buffer = ChannelBuffers.dynamicBuffer( ByteOrder.LITTLE_ENDIAN, 32 );
        buffer.writeInt( 0 ); // write length later

        final MongoReply reply = (MongoReply) msg;

        buffer.writeInt( reply.getHeader().getRequestID() );
        buffer.writeInt( reply.getHeader().getResponseTo() );
        buffer.writeInt( OpCode.OP_REPLY.getId() );

        buffer.writeInt( reply.getFlags() );
        buffer.writeLong( reply.getCursorId() );
        buffer.writeInt( reply.getStartingFrom() );
        final List<BSONObject> documents = reply.getDocuments();
        buffer.writeInt( documents.size() );

        for ( final BSONObject bsonObject : documents ) {
            buffer.writeBytes( BSON.encode( bsonObject ) );
        }

        _log.debug( "wrote reply: " + reply );

        // now set the length
        final int writerIndex = buffer.writerIndex();
        buffer.setInt( 0, writerIndex );
        return buffer;
    }
}
